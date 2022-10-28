# lua_orm

##原理：
通过lua元表hook达到监控及捕获table变更并记录到变更列表，从而实现“闲时异步”转换为sql执行

##适合场景：
适合面向用mysql类传统关系数据库，且项目里存在大量业务逻辑对象需要存储到db的场景，若用mongo之类对象型数据库，则可能没有必要；

##核心价值：
1、自动化将lua对象转换为sql，避免在业务逻辑中引入大量数据格式处理相关代码，提高开发效率，改善代码质量和维护度；
2、采用异步闲时存储机制，能适当提高业务层响应速度；
3、存储机制采用标记异步闲时延时机制，天然具备多次操作合并为单次存储的功效，能降低db的实际存储频率和次数；

##缺点：
异步更新删除机制可能导致存储发生错误或失败时无法即时让客户端和用户感知，可能造成数据丢失或回滚的假像。
