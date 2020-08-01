/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.cli.commands;

import org.apache.hudi.cli.HoodieCLI;
import org.apache.hudi.cli.testutils.AbstractShellBaseIntegrationTest;
import org.apache.hudi.exception.HoodieException;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.shell.core.CommandResult;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestTempViewCommand extends AbstractShellBaseIntegrationTest {

  private String tableName = "test_table";

  @BeforeEach
  public void init() {
    List<List<Comparable>> rows = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      rows.add(Arrays.asList(new Comparable[] {"c1", "c2", "c3"}));
    }
    HoodieCLI.getTempViewProvider().createOrReplace(tableName, Arrays.asList("t1", "t2", "t3"), rows);
  }

  @AfterAll
  public static void shutdown() {
    if (HoodieCLI.getTempViewProvider() != null) {
      HoodieCLI.closeTempViewProvider();
    }
  }

  @Test
  public void testQueryWithException() {
    CommandResult cr = getShell().executeCommand(String.format("temp query --sql 'select * from %s'", "table_1"));
    assertEquals(TempViewCommand.QUERY_FAIL, cr.getResult().toString());
  }

  @Test
  public void testQuery() {
    CommandResult cr = getShell().executeCommand(String.format("temp query --sql 'select * from %s'", tableName));
    assertEquals(TempViewCommand.QUERY_SUCCESS, cr.getResult().toString());
  }

  @Test
  public void testShowAll() {
    CommandResult cr = getShell().executeCommand("temps show");
    assertEquals(TempViewCommand.SHOW_SUCCESS, cr.getResult().toString());
  }

  @Test
  public void testDelete() {
    CommandResult cr = getShell().executeCommand(String.format("temp delete --view %s", tableName));
    assertTrue(cr.getResult().toString().endsWith("successfully!"));

    // after delete, we can not access table yet.
    assertThrows(HoodieException.class, () -> HoodieCLI.getTempViewProvider().runQuery("select * from " + tableName));
  }
}
