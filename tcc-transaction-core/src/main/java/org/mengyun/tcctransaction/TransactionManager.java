package org.mengyun.tcctransaction;

import org.apache.log4j.Logger;
import org.mengyun.tcctransaction.api.TransactionContext;
import org.mengyun.tcctransaction.api.TransactionStatus;
import org.mengyun.tcctransaction.common.TransactionType;

import java.util.Deque;
import java.util.LinkedList;
import java.util.concurrent.ExecutorService;

/**
 * Created by changmingxie on 10/26/15. ，事务管理器，提供事务的获取、发起、提交、回滚，参与者的新增等等方法
 */
public class TransactionManager {

    static final Logger logger = Logger.getLogger(TransactionManager.class.getSimpleName());

    private TransactionRepository transactionRepository;

    private static final ThreadLocal<Deque<Transaction>> CURRENT = new ThreadLocal<Deque<Transaction>>();

    private ExecutorService executorService;

    public void setTransactionRepository(TransactionRepository transactionRepository) {
        this.transactionRepository = transactionRepository;
    }

    public void setExecutorService(ExecutorService executorService) {
        this.executorService = executorService;
    }

    public TransactionManager() {
    }

    /**
     *
     * 发起根事务。该方法在调用方法类型为 MethodType.ROOT 并且 事务处于 Try 阶段被调用。MethodType 在「6.2 可补偿事务拦截器」详细解析。
     */
    public Transaction begin() {
        // 创建 根事务
        Transaction transaction = new Transaction(TransactionType.ROOT);
        // 存储 事务
        transactionRepository.create(transaction);
        // 注册 事务
        registerTransaction(transaction);
        return transaction;
    }

    /**
     * 传播发起分支事务 该方法在调用方法类型为 MethodType.PROVIDER 并且 事务处于 Try 阶段被调用。MethodType 在「可补偿事务拦截器」详细解析。
     * @param transactionContext
     * @return
     */
    public Transaction propagationNewBegin(TransactionContext transactionContext) {
// 创建 分支事务
        Transaction transaction = new Transaction(transactionContext);
        transactionRepository.create(transaction);// 存储事务  为什么要存储分支事务，在「资源协调者拦截器」详细解析
// 注册 事务
        registerTransaction(transaction);
        return transaction;
    }

    /**
     * 传播获取分支事务   该方法在调用方法类型为 MethodType.PROVIDER 并且 事务处于 Confirm / Cancel 阶段被调用。MethodType 在「可补偿事务拦截器」详细解析
     * @param transactionContext
     * @return
     * @throws NoExistedTransactionException
     */
    public Transaction propagationExistBegin(TransactionContext transactionContext) throws NoExistedTransactionException {
        // 查询 事务
        Transaction transaction = transactionRepository.findByXid(transactionContext.getXid());

        if (transaction != null) {
            // 设置 事务 状态
            transaction.changeStatus(TransactionStatus.valueOf(transactionContext.getStatus()));
            // 注册 事务
            registerTransaction(transaction);
            return transaction;
        } else {
            throw new NoExistedTransactionException();
        }
    }

    public void commit(boolean asyncCommit) {
       // 获取 事务
        final Transaction transaction = getCurrentTransaction();
        // 设置 事务状态 为 CONFIRMING
        transaction.changeStatus(TransactionStatus.CONFIRMING);
        // 更新 事务
        transactionRepository.update(transaction);

        if (asyncCommit) {//异步提交
            try {
                Long statTime = System.currentTimeMillis();

                executorService.submit(new Runnable() {
                    @Override
                    public void run() {
                        commitTransaction(transaction);
                    }
                });
                logger.debug("async submit cost time:" + (System.currentTimeMillis() - statTime));
            } catch (Throwable commitException) {
                logger.warn("compensable transaction async submit confirm failed, recovery job will try to confirm later.", commitException);
                throw new ConfirmingException(commitException);
            }
        } else {
            commitTransaction(transaction);
        }
    }

    public void syncTransaction() {

        final Transaction transaction = getCurrentTransaction();
        /**
         * update the transaction to persist the participant context info
         */
        transactionRepository.update(transaction);
    }

    /**
     * 取消事务 该方法在事务处于 Confirm / Cancel 阶段被调用
     * @param asyncRollback
     */
    public void rollback(boolean asyncRollback) {

        final Transaction transaction = getCurrentTransaction();
        transaction.changeStatus(TransactionStatus.CANCELLING);

        transactionRepository.update(transaction);

        if (asyncRollback) {

            try {
                executorService.submit(new Runnable() {
                    @Override
                    public void run() {
                        rollbackTransaction(transaction);
                    }
                });
            } catch (Throwable rollbackException) {
                logger.warn("compensable transaction async rollback failed, recovery job will try to rollback later.", rollbackException);
                throw new CancellingException(rollbackException);
            }
        } else {

            rollbackTransaction(transaction);
        }
    }


    private void commitTransaction(Transaction transaction) {
        try {
            transaction.commit();// 提交 事务
            transactionRepository.delete(transaction);// 删除 事务
        } catch (Throwable commitException) {
            logger.warn("compensable transaction confirm failed, recovery job will try to confirm later.", commitException);
            throw new ConfirmingException(commitException);
        }
    }

    private void rollbackTransaction(Transaction transaction) {
        try {
            transaction.rollback();// 回滚事务
            transactionRepository.delete(transaction); // 删除 事务
        } catch (Throwable rollbackException) {
            logger.warn("compensable transaction rollback failed, recovery job will try to rollback later.", rollbackException);
            throw new CancellingException(rollbackException);
        }
    }

    public Transaction getCurrentTransaction() {
        if (isTransactionActive()) {
            return CURRENT.get().peek();// 获得头部元素
        }
        return null;
    }

    public boolean isTransactionActive() {
        Deque<Transaction> transactions = CURRENT.get();
        return transactions != null && !transactions.isEmpty();
    }

//注册事务到当前线程事务队列(可能有同学会比较好奇，为什么使用队列存储当前线程事务？TCC-Transaction 支持多个的事务独立存在，
// 后创建的事务先提交，类似 Spring 的org.springframework.transaction.annotation.Propagation.REQUIRES_NEW 。
// 在下文，很快我们就会看到 TCC-Transaction 自己的 org.mengyun.tcctransaction.api.Propagation)

    private void registerTransaction(Transaction transaction) {

        if (CURRENT.get() == null) {
            CURRENT.set(new LinkedList<Transaction>());
        }

        CURRENT.get().push(transaction);// 添加到头部
    }
//清除事务
    public void cleanAfterCompletion(Transaction transaction) {
        if (isTransactionActive() && transaction != null) {
            Transaction currentTransaction = getCurrentTransaction();
            if (currentTransaction == transaction) {
                CURRENT.get().pop();
            } else {
                throw new SystemException("Illegal transaction when clean after completion");
            }
        }
    }

//添加参与者到事务
    public void enlistParticipant(Participant participant) {
        // 获取 事务
        Transaction transaction = this.getCurrentTransaction();
        // 添加参与者
        transaction.enlistParticipant(participant);
        // 更新 事务
        transactionRepository.update(transaction);
    }
}
