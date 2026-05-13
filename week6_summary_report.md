# 第六周总结报告：Data Cleaning and Preparation

## 1. 本周目标

根据 handbook 中 Weeks 6–7 的要求，本周工作的重点仍然是 **Data Cleaning and Preparation**，目标不是进入建模，而是把上一阶段已经清洗过的数据进一步整理成更可靠、更清晰、更容易解释的分析输入。

这一周我继续处理两份住宅数据集：

- `sold`：住宅成交数据
- `listed`：住宅挂牌数据

它们的输入基础来自 Week 5 已完成的 cleaned datasets，因此本周并不是从原始 CSV 重新开始，而是在 analysis-ready 基础上进一步确认：

- 日期字段是否真的完成了 datetime 转换
- 数值字段是否已经稳定地转成 numeric type
- 缺失值应该删除还是保留
- 哪些列是冗余列，哪些列虽然缺失但仍然保留业务意义

---

## 2. 本周处理的数据集来源

Week 6 的输入文件是：

- [sold_analysis_ready_week45.csv](/Users/siyulin/Desktop/Internship/idx-project/output/week4_5/sold/sold_analysis_ready_week45.csv)
- [listed_analysis_ready_week45.csv](/Users/siyulin/Desktop/Internship/idx-project/output/week4_5/listed/listed_analysis_ready_week45.csv)

这说明本周是在 Week 5 输出结果上继续做 preparation，而不是推翻前面的 cleaning 逻辑。

---

## 3. 本周做了哪些具体的数据处理

### 3.1 再次确认日期字段类型

我对以下核心日期字段重新做了 datetime conversion check：

- `CloseDate`
- `PurchaseContractDate`
- `ListingContractDate`
- `ContractStatusChangeDate`

目的不是重复做无意义操作，而是为了输出一个明确的 Week 6 交付文件，证明这些字段在当前版本的数据里已经可用于时间比较、排序和后续分析。

生成文件：

- [sold_week6_date_conversion_check.csv](/Users/siyulin/Desktop/Internship/idx-project/output/week6/sold/sold_week6_date_conversion_check.csv)
- [listed_week6_date_conversion_check.csv](/Users/siyulin/Desktop/Internship/idx-project/output/week6/listed/listed_week6_date_conversion_check.csv)

从结果看：

### Sold
- `CloseDate`：330,243 条非空，0 条空值
- `ListingContractDate`：330,243 条非空，0 条空值
- `PurchaseContractDate`：330,054 条非空，189 条空值
- `ContractStatusChangeDate`：329,654 条非空，589 条空值

### Listed
- `ListingContractDate`：478,205 条非空，0 条空值
- `CloseDate`：144,973 条非空，333,232 条空值
- `PurchaseContractDate`：234,182 条非空，244,023 条空值
- `ContractStatusChangeDate`：472,848 条非空，5,357 条空值

这里最重要的业务解释是：

- 在 `sold` 数据里，`CloseDate` 是必须字段，因此空值在前一阶段已被清除
- 在 `listed` 数据里，`CloseDate` 大量缺失是可以接受的，因为很多挂牌记录还没有真正 close

---

## 4. 数值字段类型确认

本周还专门对关键 numeric fields 做了检查，确保这些列已经可以稳定参与数值分析：

- `ClosePrice`
- `ListPrice`
- `OriginalListPrice`
- `LivingArea`
- `LotSizeAcres`
- `LotSizeSquareFeet`
- `BedroomsTotal`
- `BathroomsTotalInteger`
- `DaysOnMarket`
- `YearBuilt`
- `Latitude`
- `Longitude`

生成文件：

- [sold_week6_numeric_conversion_check.csv](/Users/siyulin/Desktop/Internship/idx-project/output/week6/sold/sold_week6_numeric_conversion_check.csv)
- [listed_week6_numeric_conversion_check.csv](/Users/siyulin/Desktop/Internship/idx-project/output/week6/listed/listed_week6_numeric_conversion_check.csv)

这些文件里不仅记录了 dtype，还记录了：

- non-null count
- null count
- min value
- max value

这样可以快速发现某些字段虽然成功转成 numeric，但数值范围仍可能异常。

例如：

### Sold
- `ClosePrice` 最小值 `1.15`，最大值 `989,500,000`
- `LivingArea` 最小值 `1`，最大值 `17,021,320`
- `Longitude` 最大值 `329`

### Listed
- `ListPrice` 最小值 `100`，最大值 `195,000,000`
- `LivingArea` 最大值同样非常大
- `Latitude` 最大值 `737`
- `Longitude` 最大值 `177`

这说明：

- dtype 虽然已经正确
- 但个别字段仍然存在极端值或坐标异常
- 这些问题不属于“类型错误”，而是“值域异常”

---

## 5. 冗余列和保留列的处理

本周并没有重新大规模 drop columns，因为主要的冗余字段已经在 Week 5 做过删除。  
Week 6 更偏向于“确认当前保留下来的列是不是合理”。

我新增了列清单文件：

- [sold_week6_column_inventory.csv](/Users/siyulin/Desktop/Internship/idx-project/output/week6/sold/sold_week6_column_inventory.csv)
- [listed_week6_column_inventory.csv](/Users/siyulin/Desktop/Internship/idx-project/output/week6/listed/listed_week6_column_inventory.csv)

这些文件记录了：

- column 名称
- dtype
- 是否属于 required field

这一步的价值在于：

- 让 Week 6 的 deliverable 更像一个正式的数据准备阶段产物
- 方便后续老师或团队成员核对当前版本到底保留了哪些列

---

## 6. 缺失值是如何处理的

这是本周最重要的部分之一。  
我没有把所有空值都当作错误，而是针对不同字段定义了不同处理方式。

生成文件：

- [sold_week6_missing_summary.csv](/Users/siyulin/Desktop/Internship/idx-project/output/week6/sold/sold_week6_missing_summary.csv)
- [listed_week6_missing_summary.csv](/Users/siyulin/Desktop/Internship/idx-project/output/week6/listed/listed_week6_missing_summary.csv)
- [sold_week6_missing_action_summary.csv](/Users/siyulin/Desktop/Internship/idx-project/output/week6/sold/sold_week6_missing_action_summary.csv)
- [listed_week6_missing_action_summary.csv](/Users/siyulin/Desktop/Internship/idx-project/output/week6/listed/listed_week6_missing_action_summary.csv)

### 6.1 `sold` 的缺失值处理逻辑

在 `sold` 数据里：

- `CloseDate` 和 `ClosePrice` 被视为 required fields
- 如果缺失，记录在 Week 5 就已经被删除
- `Latitude` / `Longitude` 缺失则保留，但标记为不能直接用于 spatial analysis
- `LotSizeAcres` / `LotSizeSquareFeet` 会尝试互相补值
- 其他可选字段，比如 `PropertySubType`、`YearBuilt`、`PurchaseContractDate`，如果缺失则保留

从结果看，`sold` 当前缺失较高的字段主要是：

- `LotSizeAcres`：25,478
- `LotSizeSquareFeet`：25,478
- `Latitude`：11,451
- `Longitude`：11,451

### 6.2 `listed` 的缺失值处理逻辑

在 `listed` 数据里，最关键的业务判断是：

- `ClosePrice`
- `CloseDate`
- `PurchaseContractDate`

这些空值很多时候是**业务上合理的**，因为挂牌房源不一定已经成交。

所以 Week 6 中我明确把这些字段标记为：

`retained nulls when business-valid for active or not-yet-closed listings`

当前缺失最明显的是：

- `ClosePrice`：350,850，约 73.37%
- `CloseDate`：333,232，约 69.68%
- `PurchaseContractDate`：244,023，约 51.03%

这也是 `listed` 和 `sold` 清洗逻辑最本质的差异。

---

## 7. 本周额外做的一个准备动作：Lot Size Cross-fill

Week 6 我还加了一个比较实用的准备动作：

- 如果 `LotSizeAcres` 缺失，但 `LotSizeSquareFeet` 有值，就用 `square feet / 43560` 换算 acres
- 如果 `LotSizeSquareFeet` 缺失，但 `LotSizeAcres` 有值，就用 `acres * 43560` 回填 square feet

这一步的意义是：

- 不必因为面积单位不同而人为增加缺失
- 保留更多土地面积信息供后续分析使用

从 Week 6 输出看，这一步已经体现在 missing action summary 中：

`cross-filled when companion lot-size field existed; remaining nulls retained`

---

## 8. 本周遇到的困难和疑问

### 困难 1：缺失值不等于错误值
最大的困难仍然是判断“空值是不是应该被删掉”。

例如：
- 在 `sold` 中缺失 `ClosePrice` 通常不能接受
- 但在 `listed` 中缺失 `ClosePrice` 反而很常见、很合理

所以同样是 null，不能统一处理。

### 困难 2：类型正确不代表值合理
Week 6 的 numeric conversion check 很清楚地暴露出一个问题：

- 字段可以成功转成 `float64`
- 但它的最大值可能非常离谱，比如 `LivingArea` 或坐标字段

这说明“类型修正”和“异常值处理”是两个不同层次的问题。

### 困难 3：坐标问题仍然存在
虽然 Week 5 已经做过 flag，但 Week 6 重新检查后依然发现：

- 坐标缺失很多
- 少数坐标虽然不是空值，但范围并不合理

这会影响后续地图分析、空间聚类或 county-level location quality 判断。

### 疑问
这一周也留下了一些后续疑问：

- 极端值应该在下周进一步处理，还是先保留？
- 坐标缺失的记录，是只在 spatial analysis 里排除，还是整体排除？
- `listed` 中长期缺失 closing 信息的记录，后续分析时应该归为 active inventory 还是单独分类？

---

## 9. 本周最终交付

### Sold Week 6
- [sold_analysis_ready_week6.csv](/Users/siyulin/Desktop/Internship/idx-project/output/week6/sold/sold_analysis_ready_week6.csv)
- [sold_week6_date_conversion_check.csv](/Users/siyulin/Desktop/Internship/idx-project/output/week6/sold/sold_week6_date_conversion_check.csv)
- [sold_week6_numeric_conversion_check.csv](/Users/siyulin/Desktop/Internship/idx-project/output/week6/sold/sold_week6_numeric_conversion_check.csv)
- [sold_week6_missing_summary.csv](/Users/siyulin/Desktop/Internship/idx-project/output/week6/sold/sold_week6_missing_summary.csv)
- [sold_week6_missing_action_summary.csv](/Users/siyulin/Desktop/Internship/idx-project/output/week6/sold/sold_week6_missing_action_summary.csv)
- [sold_week6_column_inventory.csv](/Users/siyulin/Desktop/Internship/idx-project/output/week6/sold/sold_week6_column_inventory.csv)
- [sold_week6_transformation_log.csv](/Users/siyulin/Desktop/Internship/idx-project/output/week6/sold/sold_week6_transformation_log.csv)

### Listed Week 6
- [listed_analysis_ready_week6.csv](/Users/siyulin/Desktop/Internship/idx-project/output/week6/listed/listed_analysis_ready_week6.csv)
- [listed_week6_date_conversion_check.csv](/Users/siyulin/Desktop/Internship/idx-project/output/week6/listed/listed_week6_date_conversion_check.csv)
- [listed_week6_numeric_conversion_check.csv](/Users/siyulin/Desktop/Internship/idx-project/output/week6/listed/listed_week6_numeric_conversion_check.csv)
- [listed_week6_missing_summary.csv](/Users/siyulin/Desktop/Internship/idx-project/output/week6/listed/listed_week6_missing_summary.csv)
- [listed_week6_missing_action_summary.csv](/Users/siyulin/Desktop/Internship/idx-project/output/week6/listed/listed_week6_missing_action_summary.csv)
- [listed_week6_column_inventory.csv](/Users/siyulin/Desktop/Internship/idx-project/output/week6/listed/listed_week6_column_inventory.csv)
- [listed_week6_transformation_log.csv](/Users/siyulin/Desktop/Internship/idx-project/output/week6/listed/listed_week6_transformation_log.csv)

---

## 10. 总结

第六周真正完成的是：  
**把上一周已经 cleaned 的数据，进一步整理成一个更适合后续分析、也更容易解释的数据准备版本。**

本周最重要的价值不是又“删了多少行”，而是：

- 明确证明了日期字段已经成功转成 datetime
- 明确确认了关键 numeric fields 的类型和范围
- 用表格化方式解释了每一类缺失值是怎么处理的
- 保留了挂牌数据中业务合理的空值
- 对 lot size 做了 cross-fill，减少无谓缺失

因此，Week 6 更像是一个“正式交付版的数据准备阶段”，为后面的 deeper analytics 打下了更清晰的基础。
