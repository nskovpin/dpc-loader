package ru.at_consulting.bigdata.dpc.cluster.groups

import ru.at_consulting.bigdata.dpc.dim._

/**
  * Created by NSkovpin on 07.03.2017.
  */
object GroupTraitFactory {

  def createGroupTrait(clazz: Class[_]): GroupTrait = {
    if (clazz == classOf[ProductDim]) {
      return CommonGroup
    } else if (clazz == classOf[ExternalRegionMappingDim]) {
      return DoubleGroup
    } else if (clazz == classOf[MarketingProductDim]) {
      return CommonGroup
    } else if (clazz == classOf[ProductRegionLinkDim]) {
      return DoubleGroupEasier
    } else if (clazz == classOf[RegionDim]) {
      return CommonGroup
    } else if (clazz == classOf[WebEntityDim]) {
      return CommonGroup
    } else if (clazz == classOf[ProductMapDim]) {
      return DoubleGroup
    }
    throw new RuntimeException("Don't know this class type:" + clazz)
  }

}
