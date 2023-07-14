package main

//
//import (
//	list2 "container/list"
//	"fmt"
//	"regexp"
//	"strings"
//)
//
//func main() {
//
//	list := list2.New()
//
//	var schema = "CREATE VIEW IF NOT EXISTS `iffff` (w_id_8, id_8) COMMENT \"VIEW\" AS SELECT `dt_wm_ods`.`aa01_zln_lixian_01`.`w_id_8` AS `w_id_8`, `dt_wm_ods`.`aa01_zln_lixian_01`.`id_8` AS `id_8` FROM `dt_wm_ods`.`aa01_zln_lixian_01`;"
//	var schema2 = " CREATE VIEW IF NOT EXISTS `zfcpcljhcbnjhcl` (日期, 物料名称, (物料)编码, 指标名称, 计划方案, 主副产品产量计划-粗苯-年计划产量) COMMENT \"VIEW\" AS SELECT `dt_wm_dw`.`dws_enh_indp_year_prod_plan_mxes_all`.`WM_EXTRA_TS_year_YEAR` AS `日期`, `dt_wm_dw`.`dws_enh_indp_year_prod_plan_mxes_all`.`mtrl_name` AS `物料名称`, `dt_wm_dw`.`dws_enh_indp_year_prod_plan_mxes_all`.`mtrl_code` AS `物料编码`, `dt_wm_dw`.`dws_enh_indp_year_prod_plan_mxes_all`.`index_name` AS `指标名称`, `dt_wm_dw`.`dws_enh_indp_year_prod_plan_mxes_all`.`plan_prog` AS `计划方案`, round(sum(`dt_wm_dw`.`dws_enh_indp_year_prod_plan_mxes_all`.`target_value`), 2) AS `主副产品产量计划-粗苯-年计划产量` FROM `dt_wm_dw`.`dws_enh_indp_year_prod_plan_mxes_all` WHERE `dt_wm_dw`.`dws_enh_indp_year_prod_plan_mxes_all`.`mtrl_code` = 'ENH_WL_0005' GROUP BY `dt_wm_dw`.`dws_enh_indp_year_prod_plan_mxes_all`.`WM_EXTRA_TS_year_YEAR`, `dt_wm_dw`.`dws_enh_indp_year_prod_plan_mxes_all`.`mtrl_name`, `dt_wm_dw`.`dws_enh_indp_year_prod_plan_mxes_all`.`mtrl_code`, `dt_wm_dw`.`dws_enh_indp_year_prod_plan_mxes_all`.`index_name`, `dt_wm_dw`.`dws_enh_indp_year_prod_plan_mxes_all`.`plan_prog` ORDER BY `dt_wm_dw`.`dws_enh_indp_year_prod_plan_mxes_all`.`WM_EXTRA_TS_year_YEAR` ASC, `dt_wm_dw`.`dws_enh_indp_year_prod_plan_mxes_all`.`mtrl_name` ASC, `dt_wm_dw`.`dws_enh_indp_year_prod_plan_mxes_all`.`mtrl_code` ASC, `dt_wm_dw`.`dws_enh_indp_year_prod_plan_mxes_all`.`index_name` ASC, `dt_wm_dw`.`dws_enh_indp_year_prod_plan_mxes_all`.`plan_prog` ASC ;"
//
//	list.PushBack(schema)
//	list.PushBack(schema2)
//
//	var reg = "\\((.*?)\\) "
//
//	var compile = regexp.MustCompile(reg)
//
//	for e := list.Front(); e != nil; e = e.Next() {
//		ddl := fmt.Sprintf("%v", e.Value)
//		//fmt.Println(ddl)
//
//		//data := compile.Find([]byte(ddl))
//		//fmt.Println(string(data))
//
//		//index := compile.FindAllStringIndex(schema, 1)
//
//		//columns := compile.FindString(ddl)
//		//fmt.Println(columns)
//		//split := strings.Split(columns, ",")
//		//fmt.Println(split)
//		//fmt.Printf("%q\n", split)
//
//		result := compile.FindStringSubmatch(ddl)
//
//		if len(result) > 1 {
//			fields := strings.Split(result[1], ",")
//			for i := range fields {
//				fields[i] = fmt.Sprintf("`%s`", strings.TrimSpace(fields[i]))
//			}
//			fmt.Println(strings.Join(fields, ","))
//			fmt.Println(strings.ReplaceAll(ddl, result[1], strings.Join(fields, ",")))
//		}
//	}
//
//}
