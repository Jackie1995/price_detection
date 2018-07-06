-- Q 837717
-- 这个是hive 命令行执行版本
-- 查询的目标是：截止到某个pt（入参）的强命中问题房源全量。
-- 暂时注释了随机抽样筛选以及价格低的房源筛选。

ADD jar hdfs://nn-cluster/user/bigdata/lib/dataDev-1.0.jar;
CREATE temporary function aes128DecodePublic as 'com.lianjia.datadev.udf.AES128DecodePublicUDF';

set hive.execution.engine=tez;
set hive.cli.print.header=true;  -- 设置为打印列名 

SELECT
    houseid_url, -- 房源线上地址
	house_code, -- C端房源编码
	rent_unit_code, -- B端出租单元编号
 	hdic_city_id, -- 城市
	hdic_district_id,-- 城区
	hdic_bizcircle_id, -- 商圈
	hdic_resblock_id, -- 小区
	business_id, -- 商家ID
	full_name, -- 公司全称
	apartment_code, -- 门店编码
	main_contacts_name, -- 联系人姓名
	contact_name,  -- 扩展信息表BD联系人姓名
	contact_number, -- 扩展信息表BD联系人方式
	app_source_brand, -- 品牌
	brand_name, -- 品牌中文名称
	rent_area, -- 出租面积
	rent_price_listing, -- 挂牌价
	app_plat_ctime, -- 平台上架时间
	app_plat_mtime, -- 平台随后修改时间	
	app_source_pkid, -- info表 外键
	house_number_brand_beijing, -- 商圈的第三方在线房源数量
	house_number_bizcircle_beijing, -- 品牌的北京在线房源数量
	high_price_brand_beijing, -- 品牌价格区间上限
	high_price_bizcircle_beijing, -- 商圈价格区间上限
  	low_price_brand_beijing,-- 品牌价格区间下限
    low_price_bizcircle_beijing, -- 商圈价格区间下限
	pt -- 时间分区
FROM
(
SELECT
    houseid_url, -- 房源线上地址
	house_code , -- C端房源编码
	rent_unit_code, -- B端出租单元编号
 	hdic_city_id, -- 城市
	hdic_district_id,-- 城区
	hdic_bizcircle_id, -- 商圈
	hdic_resblock_id, -- 小区
	app_source_brand, -- 品牌
	rent_area, -- 出租面积
	rent_price_listing, -- 挂牌价
	app_plat_ctime, -- 平台上架时间
	app_plat_mtime, -- 平台随后修改时间	
	app_source_pkid, -- info表 外键
	pt ,-- 时间分区
	house_number_brand_beijing, -- 商圈的第三方在线房源数量
	house_number_bizcircle_beijing, -- 品牌的北京在线房源数量	
	high_price_brand_beijing, -- 品牌价格区间上限
	high_price_bizcircle_beijing, -- 商圈价格区间上限
  	low_price_brand_beijing,-- 品牌价格区间下限
    low_price_bizcircle_beijing -- 商圈价格区间下限
FROM
(
SELECT 
    houseid_url, -- 房源线上地址
	house_code , -- C端房源编码
 	hdic_city_id, -- 城市
	hdic_district_id,-- 城区
	hdic_bizcircle_id, -- 商圈
	hdic_resblock_id, -- 小区
	app_source_brand, -- 品牌
	rent_area, -- 出租面积
	rent_price_listing, -- 挂牌价
	app_plat_ctime, -- 平台上架时间
	app_plat_mtime, -- 平台随后修改时间	
	app_source_pkid, -- info表 外键
	pt ,-- 时间分区
	house_number_brand_beijing, -- 商圈的第三方在线房源数量
	house_number_bizcircle_beijing, -- 品牌的北京在线房源数量
	high_price_brand_beijing, -- 品牌价格区间上限
	high_price_bizcircle_beijing, -- 商圈价格区间上限	
  	low_price_brand_beijing,-- 品牌价格区间下限
    low_price_bizcircle_beijing -- 商圈价格区间下限
FROM
(
SELECT
houseid_url,
house_code , -- C端房源编码
hdic_city_id, -- 城市
hdic_district_id,-- 城区
hdic_bizcircle_id, -- 商圈
hdic_resblock_id, -- 小区
app_source_brand, -- 品牌
rent_area, -- 出租面积
rent_price_listing, -- 挂牌价
app_plat_ctime, -- 平台上架时间
app_plat_mtime, -- 平台随后修改时间
app_source_pkid, -- info表 外键
pt, -- 时间分区
house_number_brand_beijing, -- 商圈的第三方在线房源数量
house_number_bizcircle_beijing, -- 品牌的北京在线房源数量
high_price_brand_beijing, -- 品牌价格区间上限
high_price_bizcircle_beijing, -- 商圈价格区间上限
low_price_brand_beijing,
low_price_bizcircle_beijing,
(rent_price_listing < low_price_brand_beijing or rent_price_listing > high_price_brand_beijing) AS if_yichang_brand,
(rent_price_listing < low_price_bizcircle_beijing or rent_price_listing > high_price_bizcircle_beijing) AS if_yichang_bizcircle,
rent_price_listing/low_price_brand_beijing AS price_low_brand_bili,
rent_price_listing/low_price_bizcircle_beijing AS price_low_bizcircle_bili
FROM
(
SELECT
  	CASE WHEN house_code != '' THEN concat('http://m.zufangzi.com/', substr(house_code, 0, 2), '/zufang/', house_code, '.html')
		 ELSE ''
	END AS houseid_url, -- 房源线上地址
	house_code , -- C端房源编码
	hdic_city_id, -- 城市
	hdic_district_id,-- 城区
	hdic_bizcircle_id, -- 商圈
	hdic_resblock_id, -- 小区
	app_source_brand, -- 品牌
	rent_area, -- 出租面积
	rent_price_listing, -- 挂牌价
	app_plat_ctime, -- 平台上架时间
	app_plat_mtime, -- 平台随后修改时间	
	app_source_pkid, -- info表 外键
	pt -- 时间分区
FROM
	ods.ods_rent_plat_rent_house_info_da
WHERE
	pt='${hiveconf:pt}000000' AND 
	house_type  = 107500000003 AND -- 普通住宅
	app_source_brand!='200301001000' AND app_source_brand!='200302002000' AND -- 去掉自如和链家的房源数据
	app_source!='200100000001'AND -- 去掉爬虫数据源
	hdic_city_id ='110000' AND -- 限定城市在北京
	rent_type = '200600000002' AND-- 限定租赁类型为合租 
	house_status = '202300000001' -- 限定为目前上线在租的房屋
)	as original
LEFT JOIN
(
SELECT
	hdic_city_id AS hdic_city_id2, -- （城市，品牌）作为主键来分组
	app_source_brand AS app_source_brand2, 
	count(*) as house_number_brand_beijing, -- 组内的房源数目
	round(avg(rent_price_listing),1) mean_price_brand_beijing, -- 组内房源的均价
	percentile_approx(rent_price_listing,0.25) Q1_price_brand_beijing, -- 组内房源的Q1价格
	percentile_approx(rent_price_listing,0.75) Q3_price_brand_beijing, -- 祖内房源的Q3价格
	round(std(rent_price_listing),1) std_price_brand_beijing,-- 组内房源的价格标准差
	percentile_approx(rent_price_listing,0.75)-percentile_approx(rent_price_listing,0.25) IQR_price_brand_beijing, -- 组内房源的价格四分位距
	percentile_approx(rent_price_listing,0.25)-0.5*(percentile_approx(rent_price_listing,0.75)-percentile_approx(rent_price_listing,0.25)) low_price_brand_beijing, -- 组内合理价格下界
	percentile_approx(rent_price_listing,0.75)+0.5*(percentile_approx(rent_price_listing,0.75)-percentile_approx(rent_price_listing,0.25)) high_price_brand_beijing, -- 组内合理价格上界
	2*(percentile_approx(rent_price_listing,0.75)-percentile_approx(rent_price_listing,0.25)) diff_high_low_brand_beijing
FROM
	ods.ods_rent_plat_rent_house_info_da
WHERE
	pt='${hiveconf:pt}000000' AND 
	house_type  = 107500000003 AND -- 普通住宅
	app_source_brand!='200301001000' AND app_source_brand!='200302002000' AND -- 去掉自如和链家的房源数据
	app_source!='200100000001'AND -- 去掉爬虫数据源
	hdic_city_id ='110000' AND -- 限定城市在北京
	rent_type = '200600000002' -- 限定租赁类型为合租
GROUP BY
	hdic_city_id,
	app_source_brand
HAVING
	2*(percentile_approx(rent_price_listing,0.75)-percentile_approx(rent_price_listing,0.25)) > 300.00	
) as brand_price_qujian
ON original.app_source_brand = brand_price_qujian.app_source_brand2
LEFT JOIN
(
SELECT
	hdic_city_id AS hdic_city_id3, -- （城市，商圈）作为主键来分组
	hdic_bizcircle_id AS hdic_bizcircle_id2, 
	count(*) as house_number_bizcircle_beijing, -- 组内的房源数目
	round(avg(rent_price_listing),1) mean_price_bizcircle_beijing, -- 组内房源的均价
	percentile_approx(rent_price_listing,0.25) Q1_price_bizcircle_beijing, -- 组内房源的Q1价格
	percentile_approx(rent_price_listing,0.75) Q3_price_bizcircle_beijing, -- 祖内房源的Q3价格
	round(std(rent_price_listing),1) std_price_bizcircle_beijing, -- 组内房源的价格标准差
	percentile_approx(rent_price_listing,0.75)-percentile_approx(rent_price_listing,0.25) IQR_price_bizcircle_beijing, -- 组内房源的价格四分位距
	percentile_approx(rent_price_listing,0.25)-0.5*(percentile_approx(rent_price_listing,0.75)-percentile_approx(rent_price_listing,0.25)) low_price_bizcircle_beijing, -- 组内合理价格下界
	percentile_approx(rent_price_listing,0.75)+0.5*(percentile_approx(rent_price_listing,0.75)-percentile_approx(rent_price_listing,0.25)) high_price_bizcircle_beijing, -- 组内合理价格上界
	2*(percentile_approx(rent_price_listing,0.75)-percentile_approx(rent_price_listing,0.25)) diff_high_low_bizcircle_beijing
FROM
	ods.ods_rent_plat_rent_house_info_da
WHERE
	pt='${hiveconf:pt}000000' AND 
	house_type  = 107500000003 AND -- 普通住宅
	app_source_brand!='200301001000' AND app_source_brand!='200302002000' AND -- 去掉自如和链家的房源数据
	app_source!='200100000001'AND -- 去掉爬虫数据源
	hdic_city_id ='110000' AND -- 限定城市在北京
	rent_type = '200600000002' -- 限定租赁类型为合租
GROUP BY
	hdic_city_id,
	hdic_bizcircle_id
HAVING
	2*(percentile_approx(rent_price_listing,0.75)-percentile_approx(rent_price_listing,0.25)) > 300	
) as bizcircle_price_qujian
ON original.hdic_bizcircle_id = bizcircle_price_qujian.hdic_bizcircle_id2
  
) AS X

WHERE 
	if_yichang_brand = true AND
	if_yichang_bizcircle = true -- AND
	-- price_low_brand_bili < 0.8 AND
	-- price_low_bizcircle_bili < 0.8
) AS A

LEFT JOIN 

(
	SELECT rent_unit_code
	FROM ods.ods_rpms_rent_unit_da
	WHERE pt = '${hiveconf:pt}000000'
		AND state = 2
) AS B
ON A.app_source_pkid = B.rent_unit_code
WHERE rent_unit_code IS NOT NULL
) AS C

LEFT JOIN 
(
	SELECT c_rent_unit_code, business_id , b_house_or_apartment_code AS apartment_code
	FROM rentplat.rentplat_dw_b_c_house_code_map_da
	WHERE pt = '${hiveconf:pt}000000'
) BB
ON C.house_code = BB.c_rent_unit_code

LEFT JOIN
(
	SELECT id, full_name, main_contacts_name
	FROM ods.ods_rpms_business_da
	WHERE pt = '${hiveconf:pt}000000'
) CC
ON BB.business_id = CC.id

LEFT JOIN 
(
	SELECT code, name AS brand_name
	FROM ods.ods_rpms_business_brand_da
	WHERE pt = '${hiveconf:pt}000000'
) F
ON C.app_source_brand = F.code

LEFT JOIN 
(
	SELECT house_code AS house_code_comment, aes128DecodePublic(contact_name) AS contact_name, aes128DecodePublic(contact_number) AS contact_number
	FROM ods.ods_rent_plat_rent_house_comment_da
	WHERE pt = '${hiveconf:pt}000000'
		AND app_source_brand != '200302002000'
		AND app_source_brand != '200301001000'
		AND app_source != '200100000001'
) N
ON C.house_code = N.house_code_comment
-- ORDER BY  RAND()
-- limit 100