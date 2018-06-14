package org.mengyun.tcctransaction.api;

/**
 * Created by changmingxie on 10/28/15.
 */
public enum TransactionStatus {

    TRYING(1), CONFIRMING(2), CANCELLING(3);

    private int id;

     TransactionStatus(int id) {
        this.id = id;
    }

    public int getId() {
        return id;
    }

    public static TransactionStatus valueOf(int id) {

        switch (id) {
            case 1:
                return TRYING;//尝试中状态
            case 2:
                return CONFIRMING;//确认中状态
            default:
                return CANCELLING;//取消中状态
        }
    }

}
