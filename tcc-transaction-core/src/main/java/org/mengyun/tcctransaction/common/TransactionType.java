

package org.mengyun.tcctransaction.common;

/**
 * Created by changmingxie on 11/15/15.
 */
public enum TransactionType {

    ROOT(1),
    BRANCH(2);

    int id;

    TransactionType(int id) {
        this.id = id;
    }

    public int getId() {
        return id;
    }

    public static TransactionType valueOf(int id) {
        switch (id) {
            case 1:
                return ROOT;//根事务
            case 2:
                return BRANCH;//分支事务
            default:
                return null;
        }
    }

}
