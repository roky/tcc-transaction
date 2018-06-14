package org.mengyun.tcctransaction;


import org.mengyun.tcctransaction.api.TransactionContext;
import org.mengyun.tcctransaction.api.TransactionStatus;
import org.mengyun.tcctransaction.api.TransactionXid;
import org.mengyun.tcctransaction.common.TransactionType;

import javax.transaction.xa.Xid;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by changmingxie on 10/26/15.
 */
public class Transaction implements Serializable {

    private static final long serialVersionUID = 7291423944314337931L;

    private TransactionXid xid;//事务编号

    private TransactionStatus status;//事务状态

    private TransactionType transactionType;// 事务类型

    private volatile int retriedCount = 0;// 事重试次数。在 TCC 过程中，可能参与者异常崩溃，这个时候会进行重试直到成功或超过最大次数 在《TCC-Transaction 源码解析 —— 事务恢复》详细解析

    private Date createTime = new Date();//创建时间

    private Date lastUpdateTime = new Date();//最后更新时间

    private long version = 1;//版本号,用于乐观锁更新事务  在《TCC-Transaction 源码解析 —— 事务存储器》详细解析

    private List<Participant> participants = new ArrayList<Participant>();//参与者集合

    private Map<String, Object> attachments = new ConcurrentHashMap<String, Object>();//附带属性映射 在《TCC-Transaction 源码解析 —— Dubbo 支持》

    public Transaction() {

    }

    public Transaction(TransactionContext transactionContext) {
        this.xid = transactionContext.getXid();// 事务上下文的 xid
        this.status = TransactionStatus.TRYING; // 尝试中状态
        this.transactionType = TransactionType.BRANCH;// 分支事务
    }

//创建指定类型的事务
    public Transaction(TransactionType transactionType) {
        this.xid = new TransactionXid();
        this.status = TransactionStatus.TRYING;//尝试中状态
        this.transactionType = transactionType;
    }
//添加参与者
    public void enlistParticipant(Participant participant) {
        participants.add(participant);
    }


    public Xid getXid() {
        return xid.clone();
    }

    public TransactionStatus getStatus() {
        return status;
    }


    public List<Participant> getParticipants() {
        return participants;
    }

    public TransactionType getTransactionType() {
        return transactionType;
    }

    public void changeStatus(TransactionStatus status) {
        this.status = status;
    }

//提交 TCC 事务
    public void commit() {

        for (Participant participant : participants) {
            participant.commit();
        }
    }
//回滚 TCC 事务
    public void rollback() {
        for (Participant participant : participants) {
            participant.rollback();
        }
    }

    public int getRetriedCount() {
        return retriedCount;
    }

    public void addRetriedCount() {
        this.retriedCount++;
    }

    public void resetRetriedCount(int retriedCount) {
        this.retriedCount = retriedCount;
    }

    public Map<String, Object> getAttachments() {
        return attachments;
    }

    public long getVersion() {
        return version;
    }

    public void updateVersion() {
        this.version++;
    }

    public void setVersion(long version) {
        this.version = version;
    }

    public Date getLastUpdateTime() {
        return lastUpdateTime;
    }

    public void setLastUpdateTime(Date date) {
        this.lastUpdateTime = date;
    }

    public Date getCreateTime() {
        return createTime;
    }

    public void updateTime() {
        this.lastUpdateTime = new Date();
    }


}
