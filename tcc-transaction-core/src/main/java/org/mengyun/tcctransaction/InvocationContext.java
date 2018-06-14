package org.mengyun.tcctransaction;

import java.io.Serializable;

/**
 * Created by changmingxie on 11/9/15.
 *
 * InvocationContext，执行方法调用上下文，记录类、方法名、参数类型数组、参数数组。
 * 通过这些属性，可以执行提交 / 回滚事务。在 org.mengyun.tcctransaction.Terminator 会看到具体的代码实现。
 * 本质上，TCC 通过多个参与者的 try / confirm / cancel 方法，实现事务的最终一致性
 */
public class InvocationContext implements Serializable {

    private static final long serialVersionUID = -7969140711432461165L;
    private Class targetClass;

    private String methodName;

    private Class[] parameterTypes;

    private Object[] args;

    public InvocationContext() {

    }

    public InvocationContext(Class targetClass, String methodName, Class[] parameterTypes, Object... args) {
        this.methodName = methodName;
        this.parameterTypes = parameterTypes;
        this.targetClass = targetClass;
        this.args = args;
    }

    public Object[] getArgs() {
        return args;
    }

    public Class getTargetClass() {
        return targetClass;
    }

    public String getMethodName() {
        return methodName;
    }

    public Class[] getParameterTypes() {
        return parameterTypes;
    }
}
