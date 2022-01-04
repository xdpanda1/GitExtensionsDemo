package com.wooduan.ssjj2.logic.cache;

import com.alibaba.fastjson.JSON;
import com.wooduan.ssjj2.common.cache.manager.AbstractCacheManagerService;
import com.wooduan.ssjj2.common.cache.manager.AbstractCacheUpdateOperator;
import com.wooduan.ssjj2.common.cache.manager.AsyncFlushCacheRunnable;
import com.wooduan.ssjj2.common.cache.manager.ICacheUpdateInterface;
import com.wooduan.ssjj2.common.entity.IEntity;
import com.wooduan.ssjj2.common.entity.Role;

import com.wooduan.ssjj2.common.redis.GlobalRedisService;
import com.wooduan.ssjj2.logic.cache.statistics.FlushCacheInterFaceDto;
import com.wooduan.ssjj2.logic.cache.statistics.FlushDataDto;
import com.wooduan.ssjj2.logic.cache.statistics.RoleWaitFlushDataDto;
import com.wooduan.ssjj2.logic.config.LogicConfig;
import com.wooduan.ssjj2.logic.config.LogicServerConfig;
import com.wooduan.ssjj2.logic.service.role.RoleManagerService;
import com.wooduan.ssjj2.logic.service.role.RoleService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;

/**
 * 缓存管理服务
 *
 * @author Stone Mack
 * @date 2018/8/23
 */
@Service
public class CacheManagerService extends AbstractCacheManagerService {
    private static final Logger logger = LoggerFactory.getLogger(CacheManagerService.class);
    @Autowired
    private RoleManagerService roleManagerService;
    @Autowired
    private RoleService roleService;
    @Autowired
    private List<ICacheUpdateInterface> repositoryCacheUpdateList;
    @Autowired
    private LogicConfig logicServerConfig;
    @Autowired
    private GlobalRedisService globalRedisService;
    /** 角色下一次写入数据库的时间戳(key=roleId,value=下一次写入数据库的时间戳) */
    private Map<Long,Long> roleNextFlushDbTimeCache = new ConcurrentHashMap<>();

    @PostConstruct
    void init(){
        final boolean flushDbLostTimeFlag = logicServerConfig.isDisplayFlushDbLostTime();
        this.initStart(repositoryCacheUpdateList);
        // 设置耗时状态打印
        this.setDisplayFlushDbLostTime(flushDbLostTimeFlag);
    }

    @Override
    protected <T> Collection<AsyncFlushCacheRunnable> shutDownForceFlushDbRunnable(BiConsumer<T, Float> flushDbFinishConsumer) {
        // 所有在线的玩家提交下一次数据刷新
        final Collection<Role> onlineRoleList = roleManagerService.getAllOnlineRole();
        BiConsumer<Role, Float> consumer = roleAsyncFlushFinishConsumer();
        BiConsumer<Role, Float> roleExConsumer = (o, o2) -> {
            flushDbFinishConsumer.accept((T)o,o2);
        };
        roleExConsumer.andThen(consumer);
        return buildAsyncFlushRunnablePlantB(onlineRoleList, roleExConsumer, true);
    }

    @Override
    protected Collection<AsyncFlushCacheRunnable> checkFlushRunnable() {
        // 所有在线的玩家提交下一次数据刷新
        final Collection<Role> onlineRoleList = roleManagerService.getAllOnlineRole();
        final BiConsumer<Role, Float> consumer = roleAsyncFlushFinishConsumer();
        return buildAsyncFlushRunnablePlantB(onlineRoleList, consumer, false);
    }

    @Override
    public BiConsumer<Role, Float> afterFlushDBHandle() {
        return (role, lostTime) -> {
            if (logger.isDebugEnabled()) {
                logger.debug("收到-系统关闭通知---玩家数据刷新到数据库..roleId={},lostTime={}", role.getId(), lostTime);
            }
        };
    }

    private List<AsyncFlushCacheRunnable> buildAsyncFlushRunnablePlantB(final Collection<Role> onlineRoleList,
                                                                        final BiConsumer<Role, Float> consumer, boolean isShutdown) {
        if (onlineRoleList == null || onlineRoleList.isEmpty()) {
            return Collections.emptyList();
        }
        List<AsyncFlushCacheRunnable> result = new ArrayList<>();
        for (final Role role : onlineRoleList) {
            AsyncFlushCacheRunnable flushCacheThread = this.checkRoleFlushCacheTask(role, isShutdown, consumer);
            if (flushCacheThread != null) {
                result.add(flushCacheThread);
            }
        }
        return result;
    }

    private <T> AsyncFlushCacheRunnable checkRoleFlushCacheTask(final Role role, boolean isShutdown,
                                                            final BiConsumer<Role, Float> consumer) {
        final long roleId = role.getId();
        final long currentTime = System.currentTimeMillis();
        final Long nextFlushCacheTime = roleNextFlushDbTimeCache.get(roleId);
        if (isShutdown) {
            return new AsyncFlushCacheRunnable(roleId, role, consumer,
                    this.repositoryCacheUpdateList,this.displayFlushDbLostTime);
        }
        if ((nextFlushCacheTime == null) || (nextFlushCacheTime.longValue() <= currentTime)) {
            // 下次刷新时间
            final long nextFlushDbTime = currentTime + role.getFixedDelayFlushDbTime();
            roleNextFlushDbTimeCache.put(roleId, nextFlushDbTime);
            if (logger.isDebugEnabled()) {
                logger.debug("[checkRoleFlushCacheTask] add AsyncFlushCacheThread to Runnable....roleId={}, fixedDelayFlushDbTime={}, currentTime={}, currentFlushCacheTime={}, nextFlushDbTime={}"
                        , roleId, role.getFixedDelayFlushDbTime(), new Date(currentTime), (nextFlushCacheTime != null ? new Date(nextFlushCacheTime) : null), new Date(nextFlushDbTime));
            }
            return new AsyncFlushCacheRunnable(roleId, role, consumer,
                    this.repositoryCacheUpdateList,this.displayFlushDbLostTime);
        }
        return null;
    }

    /**
     * 角色下线是的强制刷新
     */
    public CompletableFuture<Role> logoutSyncFlushDataToDbOfCallback(final Role role){
        final long roleId = role.getId();
        AsyncFlushCacheRunnable flushRunnable = new AsyncFlushCacheRunnable(roleId, role, roleAsyncFlushFinishConsumer(),
                this.repositoryCacheUpdateList,this.displayFlushDbLostTime);
        return super.logoutSyncFlushDataToDbOfCallback(flushRunnable);
    }

    /**
     * 数据写入完成后的回调方法(1.如果是gm玩家则立马清理玩家数据,要不只记录本次数据回写累积耗时)
     */
    private BiConsumer<Role, Float> roleAsyncFlushFinishConsumer(){
        return (role, lostTime) ->{
//            Role role = (Role)data;
            logger.debug("回写逻辑-[1], 玩家数据回写操作完成....");
            final long roleId = role.getId();
            if(role.getGmLoadFlag()){
                String userName = role.getUserName();
                roleManagerService.removeRole(roleId);
                roleService.removeRoleBusData(roleId);

                logger.warn("AsyncFlushCacheThread.cleanGmLoadRole roleId={}, userName={}, gmLoadFlag={}",roleId, userName, role.getGmLoadFlag());
            }
            if(logger.isDebugEnabled()){
                logger.debug("roleAsyncFlushFinishConsumer....roleId={}, totalLostTime={}", roleId, lostTime);
            }
        };
    }

    /**
     * 获取当前系统中待刷新到数据库的数据信息
     */
    public List<RoleWaitFlushDataDto> fetchAllWaitFlushDbData(){
        final Collection<Role> onlineRoleList = roleManagerService.getAllOnlineRole();
        if(onlineRoleList.isEmpty()){
            return Collections.emptyList();
        }
        List<RoleWaitFlushDataDto> result = new ArrayList<>(onlineRoleList.size());
        for(final Role role : onlineRoleList){
            RoleWaitFlushDataDto waitFlushDataDto = this.buildRoleWaitFlushDataDto(role);
            result.add(waitFlushDataDto);
        }
        return result;
    }

    /**
     * 获取当前系统中待刷新到数据库的数据信息
     */
    public RoleWaitFlushDataDto fetchAllWaitFlushDbDataByRoleId(final long roleId){
        final Role onlineRole = roleManagerService.getRole(roleId);
        if(onlineRole == null){
            return null;
        }
        return this.buildRoleWaitFlushDataDto(onlineRole);
    }

    /**
     * 使玩家的缓存失效
     * @param roleId 角色id
     */
    public void invalidateCache(long roleId) {
        for(ICacheUpdateInterface iCacheUpdateInterface : repositoryCacheUpdateList) {
            iCacheUpdateInterface.invalidateCache(roleId);
        }
    }

    private RoleWaitFlushDataDto buildRoleWaitFlushDataDto(final Role role){
        final long roleId = role.getId();
        final long currentTime = System.currentTimeMillis();
        final long nextFlushDbTime = currentTime + role.getFixedDelayFlushDbTime();
        final Long nextFlushCacheTime = roleNextFlushDbTimeCache.get(roleId);
        final long nexFlushTime = (nextFlushCacheTime == null) ? nextFlushDbTime : nextFlushCacheTime.longValue();
        RoleWaitFlushDataDto waitFlushDataDto = new RoleWaitFlushDataDto();
        waitFlushDataDto.setRoleId(roleId);
        waitFlushDataDto.setNextFlushDbTime(nexFlushTime);
        for(ICacheUpdateInterface cacheUpdateInterface : repositoryCacheUpdateList){
            AbstractCacheUpdateOperator cacheUpdateOperator = (AbstractCacheUpdateOperator) cacheUpdateInterface;
            List<IEntity> changeEntityList = cacheUpdateOperator.fetchChangeEntityList(roleId);
            int entitySize = (changeEntityList == null) ? 0 : changeEntityList.size();
            if(entitySize <= 0){
                continue;
            }
            FlushCacheInterFaceDto flushCacheInterFaceDto = new FlushCacheInterFaceDto();
            flushCacheInterFaceDto.setCacheUpdateInterface(cacheUpdateOperator.getPrinterName());
            flushCacheInterFaceDto.setCurrentFlushNum(entitySize);
            for(IEntity entityDto : changeEntityList){
                FlushDataDto dto = new FlushDataDto();
                dto.setId(entityDto.loadId());
                dto.setRoleId(roleId);
                dto.setName(entityDto.getClass().getSimpleName());
                Map<String,Object> xxx = entityDto.loadChangeFieldsMap();
                String toChangeDataStr = JSON.toJSONString(xxx);
                dto.setChangeData(toChangeDataStr);
                flushCacheInterFaceDto.addFlushDataDto(dto);
            }
            // 变动列表
            waitFlushDataDto.addFlushDataDto(flushCacheInterFaceDto);
        }
        return waitFlushDataDto;
    }
}
