package edu.drexel.cs.dbgroup.portal

import edu.drexel.cs.dbgroup.portal.plans.PortalPlan

package object portal {

  type Strategy = org.apache.spark.sql.catalyst.planning.GenericStrategy[PortalPlan]

}
