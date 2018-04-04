package com.wangyu.test;

import com.wangyu.test.exception.MongoOperationException;
import com.wangyu.test.util.ReflectionUtils;
import net.sf.cglib.beans.BeanMap;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;

import static org.springframework.data.mongodb.core.query.Criteria.where;

/**
 * Created by wuxiaoyan on 2016/12/6.
 */
public class MongoBaseDao<T, PK extends Serializable> {
    protected Logger LOGGER = LoggerFactory.getLogger(getClass());

    @Autowired
    protected MongoTemplate mongoTemplate;
    protected Class<T> entityClass;

    protected Map<String, Method> tMethodsOfGet = new HashMap<>();
    protected String identifierPropertyName;

    public MongoBaseDao() {
        this.entityClass = ReflectionUtils.getSuperClassGenricType(getClass());
        Field[] fields = this.entityClass.getDeclaredFields();
        try{
            for(Field field : fields){
                String methodName = "get" + field.getName().substring(0,1).toUpperCase() + field.getName().substring(1);
                Method getMethod = this.entityClass.getMethod(methodName, null);
                tMethodsOfGet.put(field.getName(), getMethod);
                if(field.isAnnotationPresent(org.springframework.data.annotation.Id.class)){
                    identifierPropertyName = field.getName();
                }
            }
            LOGGER.info("-------------------Mongo operate class " + entityClass + " init success!-----------------------------");
        } catch (NoSuchMethodException e) {
            LOGGER.error("-------------------Mongo operate class init fail!----------------------------- class Name is " + entityClass, e);
        }
    }

    public boolean save (T obj){
        try{
            mongoTemplate.save(obj);
            return true;
        }catch (Exception e){
            LOGGER.error("-------------------Mongo save obj error!----------------------------- the obj's class Name is " + entityClass, e);
            return false;
        }
    }

    public T getById(PK id){
        try{
            return mongoTemplate.findById(id, this.entityClass);
        } catch (Exception e){
            LOGGER.error("-------------------Mongo find obj by id = {} error!----------------------------- the obj's class Name is " + entityClass, id, e);
            return null;
        }
    }

    public T findUnique(Query query){
        try{
            T obj = mongoTemplate.findOne(query, this.entityClass);
            return obj;
        } catch (Exception e){
            LOGGER.error("-------------------Mongo find obj by query param = {} error!----------------------------- the obj's class Name is " + entityClass, query, e);
        }
        return null;
    }

    public boolean isExist(Query query){
        T obj = findUnique(query);
        if(obj == null){
            return false;
        }
        return true;
    }

    public List<T> find(Query query){
        List<T> resList = new LinkedList<>();
        try{
            resList = mongoTemplate.find(query, this.entityClass);
        } catch (Exception e){
            LOGGER.error("-------------------Mongo find list by query param = {} error!----------------------------- the element in list class Name is "+ entityClass, query, e);
        }
        return resList;
    }

    public List<T> getAll(String orderBy, boolean isAsc){
        Query query = new Query();
        if(isAsc){
            query.with(new Sort(Sort.Direction.ASC, orderBy));
        }else{
            query.with(new Sort(Sort.Direction.DESC, orderBy));
        }
        return find(query);
    }

    public boolean update(T obj) throws MongoOperationException {
        if(obj == null){
            throw new MongoOperationException("-------------------Mongo update error!--------class Name is " + entityClass + ", the obj is null");
        }

        Method getIdentifierMethod = tMethodsOfGet.get(identifierPropertyName);
        try {
            PK identifierValue = (PK)getIdentifierMethod.invoke(obj);
            if(identifierValue == null){
                throw new MongoOperationException("-------------------Mongo update error!--------class Name is " + entityClass + ", the obj's identifierValue is null");
            }

            Query query = new Query(where(identifierPropertyName).is(identifierValue));
            Update update = new Update();
            Iterator<Map.Entry<String, Method>> methodIterator = tMethodsOfGet.entrySet().iterator();
            while (methodIterator.hasNext()) {
                Map.Entry<String, Method> entry = methodIterator.next();
                Object value = entry.getValue().invoke(obj);
                if(!entry.getKey().equals(identifierPropertyName)){
                    if(value != null){
                        update.set(entry.getKey(), value);
                    }
                }
            }

            mongoTemplate.updateFirst(query, update, this.entityClass);
            return true;
        } catch (IllegalAccessException e) {
            LOGGER.error("-------------------Mongo update error!----------------------------- reflect error, class Name is "+ entityClass, e);
        } catch (InvocationTargetException e) {
            LOGGER.error("-------------------Mongo update error!----------------------------- reflect error, class Name is "+ entityClass, e);
        } catch (Exception e){
            LOGGER.error("-------------------Mongo update error!----------------------------- reflect error, class Name is "+ entityClass, e);
        }
        return false;
    }

    public boolean isExist(T obj){
        if(obj == null){
            return false;
        }

        Query query = createQuery(obj);
        return isExist(query);
    }

    public Query createQuery(T obj) {
        Query query = new Query();
        if(obj == null){
            return query;
        }

        Criteria criteria = null;
        try{
            Iterator<Map.Entry<String, Method>> methodIterator = tMethodsOfGet.entrySet().iterator();
            while (methodIterator.hasNext()) {
                Map.Entry<String, Method> entry = methodIterator.next();
                    Object value = entry.getValue().invoke(obj);
                if(value != null){
                    if(criteria == null){
                        criteria = Criteria.where(entry.getKey()).is(value);
                    }else{
                        criteria.and(entry.getKey()).is(value);
                    }
                }
            }
        } catch (InvocationTargetException e) {
            LOGGER.error("-------------------Mongo query error!----------------------------- reflect error, class Name is "+ entityClass, e);
        } catch (IllegalAccessException e) {
            LOGGER.error("-------------------Mongo query error!----------------------------- reflect error, class Name is "+ entityClass, e);
        }
        if(criteria != null){
            query.addCriteria(criteria);
        }
        return query;
    }

    @Deprecated
    public boolean update(Query query, Update update){
        try{
            mongoTemplate.updateFirst(query, update, this.entityClass);
            return true;
        }catch (Exception e){
            LOGGER.error("-------------------Mongo update error!---------class Name is " + entityClass + ", query = {} , update = {}", query, update, e);
        }
        return false;
    }

    public boolean updateWithCriteria(Criteria criteria, T targetObj){
        Query query = new Query(criteria);
        Update update = new Update();

        if(targetObj == null){
            return true;
        }

        BeanMap targetInfo = BeanMap.create(targetObj);
        try{
            Iterator<Map.Entry<String, Object>> propertyIterator =targetInfo.entrySet().iterator();
            while(propertyIterator.hasNext()){
                Map.Entry<String, Object> entry = propertyIterator.next();
                if(entry.getValue() != null){
                    update.set(entry.getKey(), entry.getValue());
                }
            }

            mongoTemplate.updateMulti(query, update, this.entityClass);
            return true;
        } catch (Exception e){
            LOGGER.error("-------------------Mongo update error!---------class Name is " + entityClass + ", criteria = {} , update = {}", criteria, update, e);
        }
        return false;
    }

    public List<T> find(T targetObj){
        return find(targetObj, identifierPropertyName, false);//默认按照主键降序排序
    }

    public List<T> find(T targetObj, String orderBy, boolean isAsc){
        List<T> resList = new LinkedList<>();
        if(targetObj == null){
            return resList;
        }

        Query query = createQuery(targetObj);

        if(StringUtils.isNotBlank(orderBy)){
            if(isAsc){
                query.with(new Sort(Sort.Direction.ASC, orderBy));
            }else{
                query.with(new Sort(Sort.Direction.DESC, orderBy));
            }
        }

        return find(query);
    }

    public boolean remove(T obj) {
        if (obj == null) {
            return false;
        }
        Query query = createQuery(obj);
        try {
            mongoTemplate.remove(query, this.entityClass);
            return true;
        } catch (Exception e) {
            LOGGER.error("-------------------Mongo remove error!---------class Name is " + entityClass + ", obj = {} ", obj, e);
            return false;
        }
    }
}
