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

import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.search.FullTextSession;
import org.hibernate.search.Search;

import java.io.Serializable;

/**
 * @author <a href="mailto:stanley.shyiko@gmail.com">Stanley Shyiko</a>
 */
public class DefaultEntityIndexer implements EntityIndexer {

    private final SessionFactory sessionFactory;

    public DefaultEntityIndexer(SessionFactory sessionFactory) {
        this.sessionFactory = sessionFactory;
    }

    @Override
    public void index(Class entityClass, Serializable id) {
        Session session = sessionFactory.openSession();
        try {
            Object entity = session.get(entityClass, id);
            FullTextSession fullTextSession = Search.getFullTextSession(session);
            Transaction tx = fullTextSession.beginTransaction();
            try {
                if (entity != null) {
                    fullTextSession.index(entity);
                } else {
                    fullTextSession.purge(entityClass, id);
                }
                tx.commit();
            } catch (RuntimeException e) {
                tx.rollback();
            }
        } finally {
            session.close();
        }
    }
}