package simpledb;

/** A LogicalJoinNode represens the state needed of a join of two
 * tables in a LogicalQueryPlan */
public class LogicalJoinNode {

    /** The first table to join (may be null)*/
    public String t1Alias;

    /** The second table to join (may be null)*/
    public String t2Alias;
    
    /** The name of the field in t1Alias to join with */
    public String f1;

    /** The name of the field in t2Alias to join with */
    public String f2;

    /** The join predicate */
    public Predicate.Op p;

    public LogicalJoinNode() {
    }

    public LogicalJoinNode(String table1, String table2, String joinField1, String joinField2, Predicate.Op pred) {
    	t1Alias = table1;
    	t2Alias = table2;
        f1 = joinField1;
        f2 = joinField2;
        p = pred;
    }
    
    /** Return a new LogicalJoinNode with the inner and outer (t1Alias.f1
     * and t2Alias.f2) tables swapped. */
    public LogicalJoinNode swapInnerOuter() {
        Predicate.Op newp;
        if (p == Predicate.Op.GREATER_THAN)
            newp = Predicate.Op.LESS_THAN_OR_EQ;
        else if (p == Predicate.Op.GREATER_THAN_OR_EQ)
            newp = Predicate.Op.LESS_THAN;
        else if (p == Predicate.Op.LESS_THAN)
            newp = Predicate.Op.GREATER_THAN_OR_EQ;
        else if (p == Predicate.Op.LESS_THAN_OR_EQ)
            newp = Predicate.Op.GREATER_THAN;
        else 
            newp = p;
        
        LogicalJoinNode j2 = new LogicalJoinNode(t2Alias,t1Alias,f2,f1, newp);
        return j2;
    }
    
    @Override public boolean equals(Object o) {
        LogicalJoinNode j2 =(LogicalJoinNode)o;
        return (j2.t1Alias.equals(t1Alias)  || j2.t1Alias.equals(t2Alias)) && (j2.t2Alias.equals(t1Alias)  || j2.t2Alias.equals(t2Alias));
    }
    
    @Override public String toString() {
        return t1Alias + ":" + t2Alias ;//+ ";" + f1 + " " + p + " " + f2;
    }
    
    @Override public int hashCode() {
        return t1Alias.hashCode() + t2Alias.hashCode() + f1.hashCode() + f2.hashCode();
    }
}

