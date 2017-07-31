--[[
create by lj 2015-12
模块说明
    游戏逻辑服务器初始化加载数据子模块
功能
    主要负责接收从数据库加载数据到内存
修改历史
    将启动加载所有数据改为仅加载全局数据，角色相关数据在玩家进入游戏时再临时加载
    实现思路：
        先优化加载参数模式，方便后续修改效率和清晰性
        仍保持异步加载send模式
        将加载完成统计表修改为加载局部变量而非现在的模块全局变量
        加载完成回调改为局部(或zh.不同函数名模式)且带参数的以实现不同的加载机制
        添加加载表记录的where条件参数
]]

package.cpath = package.cpath .. ";game/?.so"
package.path = package.path .. ";game/?.lua;"
local require = require
local skynet = require "skynet"
local gdata = require "common.gcommon"
gdata.CMD_SEND = gdata.CMD_SEND or {};



local load_complete_callback = nil;
local global_table = {
        tb_system = true,
    };

-- local all_tables = {};
local function is_all_load_tb_complete(all_tables, uid)
    for k,v in pairs(all_tables) do
        if k ~= "load_handler_name" and ((not v) or (uid and not v[uid])) then--v=false|true or {[uid] = false|true,}
            gdata.flog(k .. " not load.");
            return false;
        end
    end
    return true;
end
--设置二维数组值
local function set_2arrary_value(array, index1, index2, value)
    if not array[index1] then
        array[index1] = {};
    end
    array[index1][index2] = value;
end
local function load_tb_role_son_table(uid, all_tables, table_name, id_name, onload_callback, no_set_role_son, complete_callback, where_t, b_total_pack)--onload_callback(role, data)
    -- print("--------------:", table_name, id_name, onload_callback, no_set_role_son, complete_callback);
    gdata.dlog("all_tables['load_handler_name'] uid 1 = " , uid,",where_t = ", where_t, ", b_total_pack = ", b_total_pack, all_tables["load_handler_name"])
    if uid then
        set_2arrary_value(all_tables, table_name, uid, false);
    else
        all_tables[table_name] = false;
    end
    if not id_name then
        id_name = "id";
    end
    if no_set_role_son then
        gdata.server_data[table_name] = gdata.server_data[table_name] or {};
    end
    -- if not gdata.CMD_SEND[table_name] then--这个是否判断理论上是一样的，因为这个函数对应table_name是固定的
        gdata.CMD_SEND[table_name] = function(address, data, uid, where_t, b_total_pack)
            -- assert(gdata.server_data.tb_role_complete)
            gdata.flog(data, uid, where_t);
            if data and not b_total_pack then
                assert(data.uid or no_set_role_son, table_name);--role的子表必须均包含uid
                local role = data.uid and (gdata.server_data.tb_role and gdata.server_data.tb_role[data.uid])
                if onload_callback then
                    onload_callback(role, data);
                end
                if not no_set_role_son then
                    -- gdata.dlog(table_name, id_name, data);
                    role[table_name] = role[table_name] or {} --如果此语句影响性能，则需要删除，并在角色加载完成事件中进行此初始化，此时只须调用一次，但每个角色子表需手工添加初始化代码
                    role[table_name][id_name and data[id_name] or 1] = data;
                end
                if not onload_callback and no_set_role_son then
                    gdata.server_data[table_name][id_name and data[id_name] or 1] = data
                end
                gdata.server_data[table_name .. "_count"] = (gdata.server_data[table_name .. "_count"] and gdata.server_data[table_name .. "_count"] or 0) + 1;
            elseif data and b_total_pack then
                for k,v in pairs(data) do
                    assert(v.uid or no_set_role_son, table_name);--role的子表必须均包含uid
                    local role = v.uid and (gdata.server_data.tb_role and gdata.server_data.tb_role[v.uid])
                    if onload_callback then
                        onload_callback(role, v);
                    end
                    if not no_set_role_son then
                        -- gdata.dlog(table_name, id_name, v);
                        role[table_name][id_name and v[id_name] or 1] = v;
                    end
                    if not onload_callback and no_set_role_son then
                        gdata.server_data[table_name][id_name and v[id_name] or 1] = v
                    end
                    gdata.server_data[table_name .. "_count"] = (gdata.server_data[table_name .. "_count"] and gdata.server_data[table_name .. "_count"] or 0) + 1;
                end
            else
                gdata.server_data[table_name .. "_complete"] = true
                gdata.flog("get " .. table_name .. " complete, count:" .. (gdata.server_data[table_name .. "_count"] and gdata.server_data[table_name .. "_count"] or 0))
                if uid then
                    all_tables[table_name][uid] = true;
                else
                    all_tables[table_name] = true;
                end
                gdata.dlog("all_tables['load_handler_name'] 2 = " , all_tables["load_handler_name"])
                if complete_callback then
                    complete_callback(uid or where_t);
                end

                if is_all_load_tb_complete(all_tables, uid) then
                    gdata.ddump(all_tables)
                    gdata.flog("all_load_tb_complete. uid = ", uid);
                    gdata.flog("where_t = ", where_t);
                    gdata.flog("all_tables.load_handler_name = ", all_tables.load_handler_name)
                    gdata.flog("no_set_role_son = " , no_set_role_son)
                    gdata.ddump(gdata.server_data._load_db_call_back)
                    if(all_tables.load_handler_name) then
                        if(not uid) then
                            gdata.dlog("r22222222222222222222222")
                            if(gdata.server_data._load_db_call_back[all_tables.load_handler_name]) then
                                gdata.dlog("r333333333333333333333333333")
                                for _,v in pairs(gdata.server_data._load_db_call_back[all_tables.load_handler_name]) do
                                    gdata.flog(" load " .. all_tables.load_handler_name .. " complete, call handler.")
                                    v(uid or where_t)
                                end
                                gdata.server_data._load_db_call_back[all_tables.load_handler_name] = nil
                            else
                                gdata.elog("no call back by load db handler[no_set_role_son = ", no_set_role_son , "] : ", all_tables.load_handler_name)
                            end
                        else
                            if(gdata.server_data._load_db_call_back[all_tables.load_handler_name]) then
                                gdata.dlog("r11111111111111111")
                                for _uid, handlers in pairs(gdata.server_data._load_db_call_back[all_tables.load_handler_name]) do
                                    -- 加载完指定uid的db后，调用回调函数
                                    gdata.dlog("_uid = " , _uid, ", type = ", type(_uid))
                                    gdata.dlog("uid = " , uid, ", type = ", type(uid))
                                    if(_uid == uid) then
                                        for _,handler in ipairs(handlers) do
                                            gdata.flog(" load " .. all_tables.load_handler_name .. tostring(uid) .. " complete, call handler.")
                                            handler(uid)
                                        end
                                        gdata.server_data._load_db_call_back[all_tables.load_handler_name][_uid] = nil
                                        break
                                    end
                                end
                                gdata.dlog("r4444444444444444444444444")
                            else
                                gdata.elog("no call back by load db handler[no_set_role_son = ", no_set_role_son , "][uid = ", uid, "] : ", all_tables.load_handler_name)
                            end
                        end
                    else
                        gdata.elog("no all_tables.load_handler_name by load db handler[no_set_role_son = ", no_set_role_son , "][uid = ", uid, "], all_tables = ", all_tables)
                    end
                    -- if(load_complete_callback) then
                    --     load_complete_callback(uid);
                    -- end
                end
            end
        end
    -- end
    gdata.flog("get_table:", table_name, uid);
    if where_t then
        gdata.senddb("get_table_record_by_table_async", table_name, uid, where_t, b_total_pack);
    elseif uid then
        gdata.senddb("get_table_record_by_uid", table_name, uid, b_total_pack);
    else
        gdata.senddb("get_table", table_name, b_total_pack);
    end
end
local function load_tb_role_son_table_param(table_name, param)
    -- param = param or {};
    load_tb_role_son_table(param.uid, param.all_tables, table_name, param.id_name, param.onload_callback, param.no_set_role_son, param.complete_callback, param.where_t, param.b_total_pack);
end

local function init_table()
    gdata.server_data._load_db_call_back = {}
end

-- 添加load_handler_name 加载完成后的回调函数
-- 当 no_set_role_son 为 false 时 uid 有效 , no_set_role_son 为 true 时 uid 无效
-- return 是否已存在 load_handler_name 加载事件
local function add_load_callback( no_set_role_son, load_handler_name, uid, load_complete_callback_ )
    if(no_set_role_son) then
        -- 已经正在加载这个load_handler_name类型的信息了，只需要添加加载回调函数就可以了。
        if(gdata.server_data._load_db_call_back[load_handler_name]) then
            table.insert(gdata.server_data._load_db_call_back[load_handler_name], load_complete_callback_)
            return true
        else
            gdata.server_data._load_db_call_back[load_handler_name] = gdata.server_data._load_db_call_back[load_handler_name] or {}
            table.insert(gdata.server_data._load_db_call_back[load_handler_name], load_complete_callback_)
            return false
        end
    else
        gdata.server_data._load_db_call_back[load_handler_name] = gdata.server_data._load_db_call_back[load_handler_name] or {}
        if(gdata.server_data._load_db_call_back[load_handler_name][uid]) then
            table.insert(gdata.server_data._load_db_call_back[load_handler_name][uid], load_complete_callback_)
            -- 已经正在加载这个用户的信息了，只需要添加加载回调函数就可以了。
            return true
        else
            gdata.server_data._load_db_call_back[load_handler_name][uid] = gdata.server_data._load_db_call_back[load_handler_name][uid] or {}        
            table.insert(gdata.server_data._load_db_call_back[load_handler_name][uid], load_complete_callback_)
            return false
        end
    end
end

function gdata.load_global_table(on_load_global_table, load_complete_callback_)--function()    
    init_table();
    local function generate_double_index_callback(table_name, parent, index1, index2)
        return function(role, data)
                if not parent[table_name][index1] then
                    parent[table_name][index1] = {}
                end
                parent[table_name][index1][index2] = data
            end;
    end
    local all_tables = {};
    all_tables["tb_system"] = false
    all_tables.load_handler_name = "load_global"

    local bisloading = add_load_callback(true, all_tables.load_handler_name, nil, load_complete_callback_)
    if(bisloading) then return end

    
    load_tb_role_son_table_param("tb_system", {all_tables = all_tables, id_name = "id", no_set_role_son = true,});--"id", nil, true);
    --TODO:添加其它全局表的加载代码
    on_load_global_table(load_tb_role_son_table_param, all_tables)
end

-- local load_role_all_data_callback_before = {};
-- function gdata.register_load_role_all_data_callback(f_before, f_before_name)--function(role)
--     if f_before then
--         table.insert(load_role_all_data_callback_before, {f = f_before, name = f_before_name});
--     end
-- end
function gdata.load_role_all_data(on_load_role_son_table, uid, load_complete_callback_)--function(uid)
    gdata.flog("load_role_all_data uid = ", uid);
    -- load_complete_callback = load_complete_callback_;
    -- load_tb_role_son_table(table_name, param.id_name, param.onload_callback, param.no_set_role_son, param.complete_callback);
    -- local load_complete_callback_old = load_complete_callback_;
    -- load_complete_callback_ = function(uid)
    --         local role = gdata.get_role(uid);
    --         if role then
    --             gdata.call_register_function("load_role_all_data_callback_before", load_role_all_data_callback_before, role)
    --         end
    --         load_complete_callback_old(uid);
    --     end;
    local tmp_callback = load_complete_callback_;
    load_complete_callback_ = function(uid)
            local data = gdata.get_role(uid);--这里对应下面的onload_callback里临时保存的role对象
            -- gdata.dlog("=======================================", uid, data);
            if data then--已经创建过角色
                rawset(gdata.server_data.tb_role, uid, nil);
                gdata.gwrap_table_obj.set_and_wrap_obj_no_new(gdata.server_data.tb_role, data.uid, data);
            end
            return tmp_callback(uid);
        end;
    local all_tables = {};
    all_tables.load_handler_name = "tb_role"
    -- gdata.ddump(all_tables)
    -- gdata.dlog(debug.traceback())

    local bisloading = add_load_callback(false, all_tables.load_handler_name, uid, load_complete_callback_)
    if(bisloading) then return end
    
    load_tb_role_son_table_param("tb_role", 
        {
            uid = uid,
            all_tables = all_tables, 
            -- id_name = "uid", 
            onload_callback = function(role, data)
                gdata.flog("tb_role onload_callback.")
                --TODO:添加其它角色表子表的初始化代码      --出于维护方便性考虑，目前暂时无须在此初始化，将在绑定相应子表时自动初始化
                -- gdata.server_data.tb_role[data.uid] = data;
                rawset(gdata.server_data.tb_role, data.uid, data);--临时保存，暂时不执行绑定操作，等所有数据加载完了再一次性绑定，这样原有代码不用改
                -- assert(gdata.server_data.tb_role[data.uid]);
                -- gdata.dlog("=======================================", data.uid, data);
            end, 
            no_set_role_son = true, 
            complete_callback = function(uid)
                gdata.flog("tb_role complete_callback. uid = ", uid)
                gdata.ddump(all_tables)
                -- gdata.dlog("----------------uid = ", uid);
                --TODO:添加其它角色表子表的加载代码
                -- load_tb_role_son_table_param("tb_prop", {uid = uid, all_tables = all_tables, id_name = "id"});
                on_load_role_son_table(load_tb_role_son_table_param, uid, all_tables)
            end
        }
        );
end

return gdata;
