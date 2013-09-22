/*
 * Copyright 2013 Stanley Shyiko
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.shyiko.rook.target.hibernate4.fulltextindex;

import org.hibernate.property.Getter;

import java.lang.reflect.Field;
import java.lang.reflect.Member;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

/**
 * @author <a href="mailto:stanley.shyiko@gmail.com">Stanley Shyiko</a>
 */
public class Reference {

    private final Class targetEntityClass;
    private final Getter getter;

    public Reference(Getter getter) {
        this.getter = getter;
        this.targetEntityClass = resolveTargetEntityClass(getter);
    }

    private Class resolveTargetEntityClass(Getter getter) {
        Class result = getter.getReturnType();
        Member member = getter.getMember();
        if (member instanceof Field) {
            Type genericType = ((Field) member).getGenericType();
            if (genericType instanceof ParameterizedType) {
                Type[] actualTypeArguments = ((ParameterizedType) genericType).getActualTypeArguments();
                if (actualTypeArguments.length == 1) {
                    result = (Class) actualTypeArguments[0];
                }
            }
            // todo: else check *To*(targetEntity)
        }
        return result;
    }

    public Class getTargetEntityClass() {
        return targetEntityClass;
    }

    public Object navigateFrom(Object entity) {
        return getter.get(entity);
    }
}
