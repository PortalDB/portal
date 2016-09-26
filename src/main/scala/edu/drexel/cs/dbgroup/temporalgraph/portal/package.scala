package edu.drexel.cs.dbgroup.temporalgraph

import edu.drexel.cs.dbgroup.temporalgraph.plans.PortalPlan

package object portal {

  type Strategy = org.apache.spark.sql.catalyst.planning.GenericStrategy[PortalPlan]

}
