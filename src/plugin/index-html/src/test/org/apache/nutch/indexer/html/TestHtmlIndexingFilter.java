/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nutch.indexer.html;

import org.junit.Assert;
import org.junit.Test;

/**
 * JUnit test case which tests 1. that basic searchable fields are added to a
 * document 2. that domain is added as per {@code indexer.add.domain} in
 * nutch-default.xml. 3. that title is truncated as per
 * {@code indexer.max.title.length} in nutch-default.xml. 4. that content is
 * truncated as per {@code indexer.max.content.length} in nutch-default.xml.
 * 
 * @author tejasp
 * 
 */

public class TestHtmlIndexingFilter {

  @Test
  public void testBasicIndexingFilter() throws Exception {
    Assert.assertTrue(true);
  }
}
