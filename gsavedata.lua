--[[
模块说明
    数据库表映射
功能
    实现内存对象和数据库表映射
开放接口：
	gdata.gsavedata.init 初始化，创建数据库存储协程，配置操作同步模式
	gdata.gsavedata.use_sync_mode 获取和配置操作同步模式
    gdata.gsavedata.db_bind 实现绑定，绑定时注意创建的对象应该为完事的数据表对象，即数据表中的必须字段必须提供键值对
    obj:db_del() 实现手动删除对象及数据表对象
	primary_feild_name_8432904321790 所有主键相关字段应添加在此表中
	filter_feild_name_084308439124932 所有内存数据表字段应添加在此表中(已经不推荐)，或以tb_或_开头的字段将自动按过滤字段(仅作内存业务处理，非映射数据库表字段)处理(建议)
说明
    绑定操作示例参见logic/gwrap_table_obj.lua
	要正常使用本模块绑定数据表对象，数据表对象需要定义主键(主键为逻辑意义上的主键，用于sql的where部分，主键可以为多个字段组合)，这些主键名称要全局区别于其它所有使用本模块绑定的表的字段名，同时，内存表字段(不会存入物理文件数据库中)名称也要全局区别于所有物理文件数据表字段名
	目前不推荐继续采用全局的primary_feild_name_8432904321790和filter_feild_name_084308439124932表配置，而建议采用db_bind接口中针对具体表的参数primary_feild_name配置
修改历史
	create by lj 2016-1-9
	modify by lj 2016-2-14
	将新建操作实现为非autoid时异步执行 by lj 2016-3-14
	删除部分调试代码，同时增加删除操作的异步机制及接口db_del_async() by lj 2016-3-15
	增加在普通lua环境下调试的模拟环境及整理测试代码 by lj 2016-3-15
	增加同步操作模式功能，可配置为同步或异步存储数据到数据库 by lj 2016-3-15
	修改延迟操作执行顺序以修正因按固定顺序执行导致业务逻辑错误和操作失败的bug，开放延迟时间参数给应用层 by lj 2016-3-16/17
	增加new_obj:force_update(force_update_feild_name_list, sync)接口及实现，以补充在特殊情况下可以手动强制保存到数据库的功能 by lj 2016-3-31/11:47
	完善支持字段设置为空功能 by lj 2016-4-7
	将新建数据修改为全部同步方式，防止数据插入失败引起后续处理异常 by lj 2016-6
	将删除对象改为全部同步方式，防止与新建操作执行次序产生混乱 by lj 2016-6-20
]]

package.cpath = package.cpath .. ";game/?.so"
package.path = package.path .. ";game/?.lua;"
local gdata = require "common.gcommon"
local skynet = gdata.skynet;

gdata.gsavedata = {}

local filter_feild_name_084308439124932 = {
	-- tb_ = true,
	-- _barrier_exp = true,
	-- _bossLostList = true,
	-- _monsterLostList = true,
	-- _cur_enter_barrier_id = true,
};
local function is_filter_feild_name(name)
	return filter_feild_name_084308439124932[name] or name:match("^tb_") or name:match("^_");
end
local primary_feild_name_8432904321790 = {
	id = true,
	uid = true,
	gid = true,
	pid = true,
	-- tal_layer = true,
	-- tal_index = true,
	-- shop_type = true,
	-- good_index = true,
	-- god_id = true,
	-- type = true,
	-- level = true,
	-- bid = true,
};
local function is_primary_feild_name(name)
	return primary_feild_name_8432904321790[name];
end
local delay_new_table_obj = {};--{obj_key = {time = ?, obj = ?,}, ...}
local delay_update_table_obj = {};
local delay_del_table_obj = {};
-- local OPERATOR_TYPE = {NEW = 1, UPDATE = 2, DEL = 3,};
-- local delay_operator = {
-- 		[OPERATOR_TYPE.NEW] = delay_new_table_obj, 
-- 		[OPERATOR_TYPE.UPDATE] = delay_update_table_obj, 
-- 		[OPERATOR_TYPE.DEL] = delay_del_table_obj, 
-- 	};
--放入server_data中主要用于调试监控查看
-- gdata.server_data.delay_update_table_obj = {};
-- gdata.server_data.delay_new_table_obj = {};
-- gdata.server_data.delay_del_table_obj = {};
local delay_save_time = 1;--5;--单位秒
local use_sync_mode = false;
local function check_monitor_table_obj_update()
	gdata.flog("begin");--"check_monitor_table_obj_update"
	local i = 0;
	while true do
		-- gdata.flog(i);
		local cur_clock = os.clock();
		i = i + 1;
		local new_obj_count = 0;
		local del_obj_count = 0;
		local update_obj_count = 0;
		-- gdata.flog("check_modify:", i, 
		-- 		gdata.nntable.count(delay_new_table_obj), 
		-- 		gdata.nntable.count(delay_update_table_obj), 
		-- 		gdata.nntable.count(delay_del_table_obj)
		-- 	);

		-- gdata.ilog(delay_table_obj);
		local operators = {};--{{time = ?, obj_key = ?, delay_table_obj = ?,},...};
		for i,delay_table_obj in ipairs({delay_new_table_obj,delay_update_table_obj,delay_del_table_obj}) do
			for k,v in pairs(delay_table_obj) do
				table.insert(operators, {time = v.time, obj_key = k, delay_table_obj = delay_table_obj,});
			end
		end
		-- gdata.flog("two");
		table.sort( operators, function(o1, o2) return o1.time < o2.time; end);
		-- gdata.flog("operators:", #operators);
		local function do_delay_task_new(obj_key, obj)
			local obj = obj.obj;--delay_new_table_obj[obj_key];
			local generate_autoid = obj._generate_autoid;
			obj._generate_autoid = nil;--释放内存
			local ret = obj:_db_new(generate_autoid);--注意如果generate_autoid不为ture，则ret也不会是id
			if ret then
				new_obj_count = new_obj_count + 1;
			else
				-- gdata.elog("new ");--数据库脚本执行已有错误日志，这时不再重复了
			end
			delay_new_table_obj[obj_key] = nil;
		end
		local function do_delay_task_update(obj_key, obj)
			local obj = obj.obj;--delay_update_table_obj[obj_key];
			if obj:_check_update(cur_clock, delay_save_time) then
				delay_update_table_obj[obj_key] = nil;
				update_obj_count = update_obj_count + 1;
			end
		end
		local function do_delay_task_del(obj_key, obj)
			local obj = obj.obj;--delay_del_table_obj[obj_key];
			-- gdata.flog("db_del_begin");
			obj:db_del(true);--bug在此，缺少参数!!!!!
			-- gdata.flog("db_del_end");
			delay_del_table_obj[obj_key] = nil;
			del_obj_count = del_obj_count + 1;
		end
		-- local map_do_delay_task = {};
		local function do_delay_task(delay_table_obj, obj_key)
			if delay_table_obj == delay_new_table_obj then
				-- gdata.flog("do_delay_task_new");
				do_delay_task_new(obj_key, delay_table_obj[obj_key]);
			elseif delay_table_obj == delay_update_table_obj then
				-- gdata.flog("do_delay_task_update");
				do_delay_task_update(obj_key, delay_table_obj[obj_key]);
			elseif delay_table_obj == delay_del_table_obj then
				-- gdata.flog("do_delay_task_del");
				do_delay_task_del(obj_key, delay_table_obj[obj_key]);
			else
				assert(false);
			end
		end
		-- gdata.flog("three");
		for i,v in ipairs(operators) do
			-- gdata.flog(i, v);
			do_delay_task(v.delay_table_obj, v.obj_key);
		end
		-- gdata.flog("four");

		-- if _1 and _2 and _3 and 
		if (0 < new_obj_count or 0 < update_obj_count or 0 < del_obj_count) then
			-- gdata.flog(40);
			gdata.ilog("new_obj_count:", new_obj_count);
			-- new_obj_count = 0;
			gdata.ilog("update_obj_count:", update_obj_count);
			-- update_obj_count = 0;
			gdata.ilog("del_obj_count:", del_obj_count);
			-- del_obj_count = 0;
		else
			-- gdata.flog(50);
			skynet.sleep(100);
		end
	end
end

function gdata.gsavedata.init(use_sync_mode, delay_save_time)
	gdata.fork(function() check_monitor_table_obj_update(); end);
	gdata.gsavedata.use_sync_mode(use_sync_mode);
	gdata.gsavedata.delay_save_time(delay_save_time);
end

function gdata.gsavedata.use_sync_mode(_use_sync_mode)
	if nil == _use_sync_mode then
		return use_sync_mode;--get
	end
	use_sync_mode = _use_sync_mode;
	return use_sync_mode;
end

function gdata.gsavedata.delay_save_time(_delay_save_time)
	if nil == _delay_save_time then
		return delay_save_time;--get
	end
	delay_save_time = _delay_save_time;
	return delay_save_time;
end

local function wrap_sql_value(v, k, t)
	if "string" == type(v) then
		v = "'" .. v .. "'";
	elseif nil == v then
		v = "null";
	elseif "table" == type(v) then
		-- gdata.flog(gdata.nndebug.dump(v));
		if t and t._table_feild_name and t._table_feild_name[k] then-- 注：此功能并不完整，在多层嵌套对象时，会有漏洞
			return "'" .. gdata.nntable.tostring_for_lua_load(v._raw()) .. "'";
		else
			gdata.elog("sql value is table:" .. k .. " = " .. gdata.nndebug.dump(t));
		end
	end
	return v;
end
--[[
使用说明：
新创建的所有db对象需要先bind，包括内存中新创建和数据库中加载两种情况
对于业务逻辑中内存中创建的新对象在bind后需调用_db_new以保存到db中，或在bind的is_new参数设置为true，使其bind后附加自动调用_db_new
_db_new,db_del,_db_update分别对应数据库中insert,delete,update操作
]]
function gdata.gsavedata.db_bind(table_name, table_obj, is_new, generate_autoid, primary_feild_name, table_feild_name)--primary_feild_name及table_feild_name格式形如：{"id", "uid",}
	-- gdata.flog("table_name, table_obj, is_new, generate_autoid, primary_feild_name, table_feild_name:", 
	-- 	table_name, table_obj, is_new, generate_autoid, primary_feild_name, table_feild_name);--
	local function list_to_list_true(src, dst)
		dst = dst or {};
		for i,v in ipairs(src) do
			dst[v] = true;
		end
		return dst;
	end
	local function update_to_db(t, k)
		-- gdata.flog(t, k);
		t._modify_list[k] = true;
		if use_sync_mode then
			t:_db_update();
		else
			rawset(t, "_last_modify", os.clock());
			delay_update_table_obj[tostring(t)] = {time = gdata.nndate_time.unique_clock(), obj = t,};
		end
		-- gdata.ilog(gdata.nndebug.dump(delay_update_table_obj));
		-- gdata.ilog("delay_update_table_obj count:", gdata.nntable.count(delay_update_table_obj));
	end
	local function bind_table_obj_value(t_parent, k_parent, v_parent)-- 注：此功能并不完整，在多层嵌套对象时，会有漏洞
		if type(v_parent) == "table" then
			-- gdata.flog(t_parent, k_parent, v_parent);
			v_parent = {
					_tab_value_obj = v_parent,
					_raw = function()
						return v_parent._tab_value_obj;
					end,
				};
			v_parent = setmetatable(
					v_parent
					, { 
						__index = function(t, k)
								-- gdata.flog(t, " get:" .. k);
								return t._raw()[k];
							end,
						__newindex = 
							function(t, k, v)
								-- gdata.flog(t, " set:" .. k .. "=" .. tostring(v));
								-- if is_filter_feild_name(k) then
								-- 	rawset(t, k, v);
								-- else
								if v ~= t._raw()[k] then--如果这里采用多层递归bind的话，在对象与string互转时会有问题，需要设置专门的转换机制才行
									t._raw()[k] = v;
									update_to_db(t_parent, k_parent);
								end
							end,
					}
				);
		end
		return v_parent;
	end
	local new_obj = {
			_table_name = table_name,
			-- _table_obj = table_obj,
			_last_modify = nil,--nil or false表示无修改，无须保存，TODO:此字段后面可考虑去掉，改为统一使用记录对象的time字段
			_modify_list = {},
			_primary_feild_name = nil,--如果为nil则使用全局primary_feild_name
			_table_feild_name = nil,--在此表中的字段将自动使用zh.nntable.tostring_for_lua_load和fromstring进行转换
			_raw = function()
					return table_obj;
				end
		};
	if primary_feild_name then
		new_obj._primary_feild_name = list_to_list_true(primary_feild_name);
	end
	-- if "tb_talent" == table_name then
	-- 	gdata.dlog("tb_talent-----------:", primary_feild_name, new_obj._primary_feild_name);
	-- end
	if table_feild_name then
		new_obj._table_feild_name = list_to_list_true(table_feild_name);
		for k,v in pairs(new_obj._raw()) do
			if new_obj._table_feild_name[k] then
				if ("string" == type(v)) then
					v = gdata.nntable.fromstring(v);--转换为内存中使用的表结构
				end
				assert("table" == type(v));
				v = bind_table_obj_value(new_obj, k, v);
				new_obj._raw()[k] = v;
			end
		end
	end
	function new_obj:_get_primary_feild_list()
		return self._primary_feild_name or primary_feild_name_8432904321790;
	end
	function new_obj:_is_primary_feild_name(name)
		-- if self._primary_feild_name then
		-- 	return self._primary_feild_name[name];
		-- end
		-- return is_primary_feild_name(name);
		return self:_get_primary_feild_list()[name];
	end
	function new_obj:_db_unbind()--此方法暂未公开
		delay_update_table_obj[tostring(self)] = nil;
		return self._raw();
	end
	-- function new_obj:_rawobj()--此方法暂未公开
	-- 	return self._raw();
	-- end
	function new_obj:_db_new(generate_autoid)--当generate_autoid为true且执行成功时将返回id，失败将为nil--此方法暂未公开
		-- gdata.flog("table_name:", table_name);--gdata.nndebug.dump(self));
		local fields = nil;
		local values = nil;
		for k,v in pairs(self._raw()) do
	        if not is_filter_feild_name(k) then
	            fields = (fields and (fields .. ",") or "") .. k;
	            values = (values and (values .. ",") or "") .. wrap_sql_value(v, k, self);
	        end
		end
		local sql = "insert into " .. table_name .. "(" .. fields .. ") values(" .. values .. ")";
	    local ret = (function() 
		    	    	if generate_autoid then
			    	    	-- tonumber(gdata.calldb("insert_table_by_autoid", sql)),
			    	    	return gdata.insert_table_by_autoid(sql);
			    	    else
		    	    		return gdata.calldb("excute_sql_row", sql);
		    	    	end
	    	    	end)();
		if generate_autoid then
			if ret then
				self._raw().id = ret;
			-- else
			-- 	ret = nil;--新建失败
			end
		else
			if 1 ~= ret then
				ret = nil;--新建失败
			end
		end
		if not ret then
			gdata.elog("新建失败");
		end
	    return ret;--注意如果generate_autoid不为ture，则ret也不会是id
	end
	function new_obj:db_del(sync)
		if true or sync or use_sync_mode then--防止与新建产生时间延迟的冲突(例如业务层面是先删除后插入，异步执行时可能变成了先插入后删除而造成逻辑错误)
			-- gdata.flog("table_name:", table_name);--gdata.nndebug.dump(self));
			local sql_where = nil;
			for k,v in pairs(self._raw()) do
		        if self:_is_primary_feild_name(k) then
		            sql_where = (sql_where and (sql_where .. " and ") or "") .. k .. "=" .. wrap_sql_value(v);
		        end
			end
			local sql = "delete from " .. table_name .. (sql_where and (" where " .. sql_where) or "");

			-- -----------------TODO:调试代码，后面考虑去掉的-------------------------
			-- local invalid_delete = {
			-- 		tb_user = true,
			-- 		tb_role = true,
			-- 		-- add other table
			-- 	};
			-- if invalid_delete[table_name] then
			-- 	gdata.elog("invalid_delete:", sql);
			-- 	return;
			-- end
			-- -----------------TODO:调试代码，后面考虑去掉的-------------------------

			-- gdata.flog("db_del_20");
		    local ret = gdata.calldb("excute_sql_row", sql);
		    -- gdata.flog("db_del_30");
			self:_db_unbind();
		    return ret;
		else
			delay_del_table_obj[tostring(self)] = {time = gdata.nndate_time.unique_clock(), obj = self,};
		end
	end
	function new_obj:_db_update()--此方法暂未公开
		-- gdata.flog("_db_update_begin");
		-- gdata.flog("table_name:", table_name);--gdata.nndebug.dump(self));
	    local FIELDS_SPLIT = ",";
	    local sql_where = "";--" where ";
	    local WHERE_SPLIT = " and ";
	    local sql = "update " .. table_name .." set ";
		for k,v in pairs(self._raw()) do
	        if self:_is_primary_feild_name(k) then
	            sql_where = sql_where .. k .. "=" .. wrap_sql_value(v) .. WHERE_SPLIT;
	        elseif self._modify_list[k] 
	        or (self._force_update_feild_name_list 
	        and ((0 == #self._force_update_feild_name_list) 
	        or self._force_update_feild_name_list[k])) then
	            sql = sql .. k .. "=" .. wrap_sql_value(v, k, self) .. FIELDS_SPLIT;
	        end
		end
		-- gdata.flog("_db_update_10");
		--此模式不方便处理强制更新字段，暂不用
		-- for k,_ in pairs(self:_get_primary_feild_list()) do
		-- 	sql_where = sql_where .. k .. "=" .. wrap_sql_value(self[k]) .. WHERE_SPLIT;
		-- end
		--补充设置为空的字段
		for k,_ in pairs(self._modify_list) do
			if nil == self[k] then
				sql = sql .. k .. "=" .. wrap_sql_value(self[k], k, self) .. FIELDS_SPLIT;
			end
		end
		-- gdata.flog("_db_update_20");
	    assert(FIELDS_SPLIT == sql:sub(-1), "sql:" .. sql);
	    -- if FIELDS_SPLIT == sql:sub(-1) then
		    if "" ~= sql_where then
		        assert(WHERE_SPLIT == sql_where:sub(-#WHERE_SPLIT));
		        sql_where = " where " .. sql_where:sub(1, -#WHERE_SPLIT - 1);
		    end
		    sql = sql:sub(1, -#FIELDS_SPLIT - 1) .. sql_where;
		    -- gdata.flog("_db_update_25");
		    local ret = gdata.calldb("excute_sql_row", sql);
		    -- gdata.flog("_db_update_26");
		-- end
		-- gdata.flog("_db_update_30");
		self._last_modify = nil;
		self._modify_list = {};
		if self._force_update_feild_name_list then
			delay_update_table_obj[tostring(self)] = nil;
			self._force_update_feild_name_list = nil;
		end
		return ret;
	end
	function new_obj:force_update(force_update_feild_name_list, sync)--force_update_feild_name_list字段作为在多层嵌套对象中可能监测不到或不方便监测更新状态时，采用手动强制更新策略以补充自动更新机制，当force_update_feild_name_list为空表“{}”时，表示更新所有字段，否则作为补充的更新字段名列表，默认为保存所有字段
		self._force_update_feild_name_list = list_to_list_true(force_update_feild_name_list or {}, self._force_update_feild_name_list);
		if sync or use_sync_mode then
			return new_obj:_db_update();
		else
			delay_update_table_obj[tostring(self)] = {time = gdata.nndate_time.unique_clock(), obj = self,};
		end
	end
	function new_obj:_check_update(cur_clock, delay_save_time)--返回值仅表示是否执行了更新操作，不论更新成功与否
		-- gdata.flog("cur_clock, delay_save_time, self._last_modify:", cur_clock, delay_save_time, self._last_modify);--
		if self._last_modify and (cur_clock - self._last_modify > delay_save_time) then
			self:_db_update();--为了防止死循环和无意义的失败尝试，此处无论成功失败均按返回true
			return true;
		end
	end
	new_obj = setmetatable(
			new_obj
			, { 
				__index = function(t, k)
						-- gdata.flog(t, " get:" .. k);
						return t._raw()[k];
					end,
				__newindex = 
					function(t, k, v)
						-- gdata.flog(t, " set:" .. k .. "=" .. tostring(v));
						if is_filter_feild_name(k) then
							rawset(t, k, v);
						elseif v ~= t._raw()[k] then--某些情况下，可能包含多层嵌套代码不能自动监测到“变更”而采取这种重复付值的方式来手动触发“变更”机制
							v = bind_table_obj_value(t, k, v);
							t._raw()[k] = v;
							update_to_db(t, k);
						end
					end,
	            __pairs = 
	                function(t)
	                    return next, t._raw(), nil;
	                end,
			}
		);
	if is_new then
		--所有创建操作统一采用同步模式，防止创建失败而产生内存和数据库不同步情况，源于一次解决创建玩家角色时，角色名冲突引发的bug导致服务器重启后无法启动：["err"] = "Duplicate entry 'r' for key 'AK_name'"
		--后面优化时可以考虑针对性的采用同步异步机制
		if true or use_sync_mode or generate_autoid then--同步执行，调用层可能会需要立即知道返回的id
			local ret = new_obj:_db_new(generate_autoid);--注意如果generate_autoid不为ture，则ret也不会是id
			if not ret then
				new_obj = nil;--新建失败
			end
		else--异步执行
			new_obj._generate_autoid = generate_autoid;
			delay_new_table_obj[tostring(new_obj)] = {time = gdata.nndate_time.unique_clock(), obj = new_obj,};
		end
	end
	return new_obj
end
function gdata.gsavedata.bind(table_name, table_obj, param)--primary_feild_name及table_feild_name格式形如：{"id", "uid",}
	param = param or {};
	return gdata.gsavedata.db_bind(table_name, table_obj, 
		param.is_new, param.generate_autoid, param.primary_feild_name, param.table_feild_name);
end

function gdata.gsavedata.test(use_sync_mode, delay_save_time, count)
	count = count or 50;
	gdata.gsavedata.init(use_sync_mode, delay_save_time);
	-- gdata.ilog(gdata.gsavedata.use_sync_mode());
	-- gdata.gsavedata.use_sync_mode(true);
	-- gdata.ilog(gdata.gsavedata.use_sync_mode());
	-- gdata.gsavedata.use_sync_mode(false);
	-- gdata.ilog(gdata.gsavedata.use_sync_mode());
	gdata.ilog(gdata.gsavedata.use_sync_mode());
	gdata.ilog(gdata.gsavedata.delay_save_time());
	local role = nil;
	-- {uid = 1, name="test", create_time=1, level=-1,
	-- 		table_feild_name = {k="v", ["1"] = 1,}};
	-- gdata.ilog(role);
	-- role = gdata.gsavedata.bind("tb_role", role, {table_feild_name = {"table_feild_name"}});
	-- gdata.ilog(role);
	-- gdata.ilog(role.create_time);
	-- gdata.ilog(role.level);
	-- role.level=0;
	-- gdata.ilog(role.level);
	for i=1,count do
		if 0.1 > math.random() then
			skynet.sleep(math.random(100, 300));
			-- return;
		end
		if not role then
			role = gdata.gsavedata.bind("tb_role", {uid = 1, name="test", create_time=1, level=-1,
				table_feild_name = {k="v", ["1"] = 1,}}
				, {is_new = (0.9 < math.random()), table_feild_name = {"table_feild_name"}});--
		end
		if role then
			role.level = i;
			role.name = tostring(math.random(10));
			if role.table_feild_name then
				role.table_feild_name[tostring(math.random(10))] = math.random(10);
			else
				role.table_feild_name = {k="v", ["1"] = 1,};
			end
			if 0.1 > math.random() then
				role:db_del();
				role = nil;
			end
		end
		-- gdata.flog(coroutine.status(sub_ct));
		-- for k,v in pairs(role._modify_list) do
		-- 	gdata.ilog(k,v);
		-- end
	end
	skynet.sleep(math.random(10));
end

return gdata;
