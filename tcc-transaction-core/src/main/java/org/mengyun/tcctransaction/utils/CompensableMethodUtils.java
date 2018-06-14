package org.mengyun.tcctransaction.utils;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.reflect.MethodSignature;
import org.mengyun.tcctransaction.api.Compensable;
import org.mengyun.tcctransaction.api.Propagation;
import org.mengyun.tcctransaction.api.TransactionContext;
import org.mengyun.tcctransaction.common.MethodType;

import java.lang.reflect.Method;

/**
 * Created by changmingxie on 11/21/15.
 */
public class CompensableMethodUtils {

    public static Method getCompensableMethod(ProceedingJoinPoint pjp) {
        Method method = ((MethodSignature) (pjp.getSignature())).getMethod();// 代理方法对象

        if (method.getAnnotation(Compensable.class) == null) {
            try {
                method = pjp.getTarget().getClass().getMethod(method.getName(), method.getParameterTypes());// 代理方法对象
            } catch (NoSuchMethodException e) {
                return null;
            }
        }
        return method;
    }

    /**
     * 计算方法类型( MethodType )的目的，可以根据不同方法类型，做不同的事务处理。
     方法类型为 MethodType.ROOT 时，发起根事务，判断条件如下二选一：
     事务传播级别为 Propagation.REQUIRED，并且当前没有事务。
     事务传播级别为 Propagation.REQUIRES_NEW，新建事务，如果当前存在事务，把当前事务挂起。此时，事务管理器的当前线程事务队列可能会存在多个事务。
     方法类型为 MethodType.ROOT 时，发起分支事务，判断条件如下二选一：
     事务传播级别为 Propagation.REQUIRED，并且当前不存在事务，并且方法参数传递了事务上下文。
     事务传播级别为 Propagation.PROVIDER，并且当前不存在事务，并且方法参数传递了事务上下文。
     当前不存在事务，方法参数传递了事务上下文是什么意思？当跨服务远程调用时，被调用服务本身( 服务提供者 )不在事务中，通过传递事务上下文参数，融入当前事务。
     方法类型为 MethodType.Normal 时，不进行事务处理。
     MethodType.CONSUMER 项目已经不再使用，猜测已废弃
     * @param propagation
     * @param isTransactionActive
     * @param transactionContext
     * @return
     */
    public static MethodType calculateMethodType(Propagation propagation, boolean isTransactionActive, TransactionContext transactionContext) {

        if ((propagation.equals(Propagation.REQUIRED) && !isTransactionActive && transactionContext == null) ||// Propagation.REQUIRED：支持当前事务，当前没有事务，就新建一个事务。
                propagation.equals(Propagation.REQUIRES_NEW)) {// Propagation.REQUIRES_NEW：新建事务，如果当前存在事务，把当前事务挂起。
            return MethodType.ROOT;
        } else if ((propagation.equals(Propagation.REQUIRED) // Propagation.REQUIRED：支持当前事务
                || propagation.equals(Propagation.MANDATORY)) // Propagation.MANDATORY：支持当前事务
                && !isTransactionActive && transactionContext != null) {
            return MethodType.PROVIDER;
        } else {
            return MethodType.NORMAL;
        }
    }

    public static MethodType calculateMethodType(TransactionContext transactionContext, boolean isCompensable) {

        if (transactionContext == null && isCompensable) {
            //isRootTransactionMethod
            return MethodType.ROOT;
        } else if (transactionContext == null && !isCompensable) {
            //isSoaConsumer
            return MethodType.CONSUMER;
        } else if (transactionContext != null && isCompensable) {
            //isSoaProvider
            return MethodType.PROVIDER;
        } else {
            return MethodType.NORMAL;
        }
    }

    public static int getTransactionContextParamPosition(Class<?>[] parameterTypes) {

        int position = -1;

        for (int i = 0; i < parameterTypes.length; i++) {
            if (parameterTypes[i].equals(org.mengyun.tcctransaction.api.TransactionContext.class)) {
                position = i;
                break;
            }
        }
        return position;
    }

    public static TransactionContext getTransactionContextFromArgs(Object[] args) {

        TransactionContext transactionContext = null;

        for (Object arg : args) {
            if (arg != null && org.mengyun.tcctransaction.api.TransactionContext.class.isAssignableFrom(arg.getClass())) {

                transactionContext = (org.mengyun.tcctransaction.api.TransactionContext) arg;
            }
        }

        return transactionContext;
    }
}
