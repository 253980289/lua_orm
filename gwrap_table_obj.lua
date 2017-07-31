--[[
create by lj 2016-2-17
模块说明
    包装数据库表对象
功能
    帮助实现自动保存功能
修改历史
    修正多层绑定时主键字段未设置成功的bug，问题由天赋表绑定触发 by lj 2016-4-6
]]

package.cpath = package.cpath .. ";game/?.so"
package.path = package.path .. ";game/?.lua;"
local gdata = require "common.gcommon"
local skynet = gdata.skynet;

gdata.gwrap_table_obj = {};

local no_new = nil;
local function wrap_tb_son_table(parent_obj, is_new, tb_son_table_name, generate_autoid, primary_feild_name, callback, layer, table_feild_name, _key)--_key为绑定内部实现使用，不作应用层接口使用，一般不要使用此值
    -- gdata.flog("wrap_tb_son_table:", layer);
    if not layer then
        layer = 1;
    end
    local function wrap_son_table(t, k, v, is_new)
        assert(not v._tb_son_table);
        local v = gdata.gsavedata.db_bind(tb_son_table_name, v, is_new, generate_autoid, primary_feild_name, table_feild_name);
        if v then--需要判断是否成功
            t._tb_son_table[generate_autoid and v.id or k] = v;
            if callback then
                assert("function" == type(callback));
                callback(t, k, v, is_new);
            end
        end
        return v;
    end
    local cur_key = _key or tb_son_table_name;
    local tb_son_table = {
            last_new = nil,
            _tb_son_table = (parent_obj._tb_son_table and parent_obj._tb_son_table or parent_obj)[cur_key] or {},
        };
    local function wrap_son_table_layer(t, k, v, is_new)
        -- gdata.flog("wrap_son_table_layer:", layer);
        if 1 == layer then
            if is_new then--先置空以便后面用作判断，这个必须有
                rawset(t, "last_new", nil);
            end
            local new_obj = wrap_son_table(t, k, v, is_new);--这里如果为阻塞调用，那么就可能存在并发问题
            if is_new then--new_obj可能为空
                rawset(t, "last_new", new_obj);--通过t.last_new获取新对象及id
            end
            return new_obj;
        else
            return wrap_tb_son_table(t, is_new, tb_son_table_name, generate_autoid, primary_feild_name, nil, layer - 1, table_feild_name, k);
        end
    end
    for k,v in pairs(tb_son_table._tb_son_table) do
        wrap_son_table_layer(tb_son_table, k, v, is_new);
    end
    setmetatable(tb_son_table--注意绑定的对象不能使用ipairs，因为未做(系统未提供)hook
        ,{
            __index = function(t, k)
                    -- gdata.flog(t, " get:" .. k);
                    return t._tb_son_table[k];
                end,
            __newindex = 
                function(t, k, v)
                    -- gdata.flog(t, " set:" .. k .. "=" .. tostring(v));
                    --[[
                    以下几种情况：
                        new:t[k]为nil,v为非nil，此时通过_last_new缓存最后对象以供查询
                        del:t[k]为非nil,v为nil
                        update:t[k]及v均非nil
                            t[k]为非绑定对象，v为绑定对象，直接替换
                            其它情况暂未考虑到，先直接使用assert(false)
                    ]]
                    if not t[k] and v then
                        local bind_obj = wrap_son_table_layer(t, k, v, not no_new and true);
                        if bind_obj then
                            gdata.server_data[tb_son_table_name .. "_count"] = (gdata.server_data[tb_son_table_name .. "_count"] and gdata.server_data[tb_son_table_name .. "_count"] or 0) + 1;
                        end
                        return;--此k无效
                    elseif t[k] and not v then
                        assert(1 == layer, "其它情况暂未考虑到，先直接使用assert");
                        -- if "tb_task" == tb_son_table_name then
                        --     gdata.flog("task will del:", t);
                        -- end
                        t[k]:db_del();
                    else
                        -- assert(t[k] and not t[k]._tb_son_table and v and v._tb_son_table, "暂未考虑的情况，执行到此再根据实际情况增加处理分支");
                        -- assert(not generate_autoid or k == v.id);
                        assert(t._tb_son_table[k] == v, (tb_son_table_name .. ":" .. k .. " = " .. gdata.nndebug.dump(v)));
                        return;
                    end
                    t._tb_son_table[k] = v;
                end,
            __pairs = 
                function(t)
                    return next, t._tb_son_table, nil;
                end,
        });
    if parent_obj._tb_son_table then
        parent_obj._tb_son_table[cur_key] = tb_son_table;
    else
        parent_obj[cur_key] = tb_son_table;
    end
    return tb_son_table;
end

--[[
parent_obj  父对象
is_new  是否为新创建该对象
tb_son_table_name   子表名
generate_autoid 该绑定表是否包含自动生成自增id字段
primary_feild_name  表的主键字段
callback    创建该对象并绑定完成时的回调，此时可能需要递归绑定其子对象
layer   该对象是父对象的第几层数组，如parent[key]为1层,parnet[key1][key2]为2层
table_feild_name    包含的这些字段为lua表结构字段，需将表转换为字符串格式存储，在加载时，需将字符串转换为内存中表来使用
_key    绑定内部实现机制使用，不作应用层接口使用
]]
local function wrap_tb_son_table_param(parent_obj, tb_son_table_name, param)
    return wrap_tb_son_table(parent_obj, param.is_new, tb_son_table_name, param.generate_autoid, param.primary_feild_name, param.callback, param.layer, param.table_feild_name, param._key);
end

-- wrap_tb_son_table(parent_obj, is_new, tb_son_table_name, generate_autoid, primary_feild_name, callback, layer, _key)
function gdata.gwrap_table_obj.wrap_all_db_obj(on_bind_global_table, on_bind_role_table)
    if not gdata.server_data.tb_role_count then
        gdata.server_data.tb_role_count = 0;
    end
    wrap_tb_son_table_param(gdata.server_data, "tb_role", {callback=
        function(t, k, vrole, is_new)
            on_bind_role_table(wrap_tb_son_table_param, vrole, is_new)
        end});
    wrap_tb_son_table_param(gdata.server_data, "tb_system", {primary_feild_name={"id"}});
    on_bind_global_table(wrap_tb_son_table_param);
end

function gdata.gwrap_table_obj.set_and_wrap_obj_no_new(t, k, v)
    no_new = true;--利用lua的单线程和协程非阻塞不切换机制
    t[k] = v;
    no_new = false;
end

function gdata.gwrap_table_obj.test(use_sync_mode, count)
    count = count or 50;
    gdata.gsavedata.init(use_sync_mode);
    -- print(gdata.gwrap_table_obj.wrap_all_db_obj);
    gdata.gwrap_table_obj.wrap_all_db_obj();
    for i=1,count do
        gdata.server_data.tb_role[i] = {uid = i, name = "x",};
        -- skynet.sleep(1);
        gdata.server_data.tb_role[i].name = tostring(i);
        -- skynet.sleep(1);
        gdata.server_data.tb_role[i] = nil;
        print("main", i);
        if 0.5 < math.random() then
            skynet.sleep(1);
        end
    end
end

return gdata;
