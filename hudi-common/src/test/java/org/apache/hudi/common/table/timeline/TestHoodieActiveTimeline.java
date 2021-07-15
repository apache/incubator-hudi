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

package org.apache.hudi.common.table.timeline;

import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant.State;
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.testutils.MockHoodieTimeline;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieException;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion.VERSION_0;
import static org.apache.hudi.common.testutils.Assertions.assertStreamEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests {@link HoodieActiveTimeline}.
 */
public class TestHoodieActiveTimeline extends HoodieCommonTestHarness {

  private HoodieActiveTimeline timeline;

  @BeforeEach
  public void setUp() throws Exception {
    initMetaClient();
  }

  @Test
  public void testLoadingInstantsFromFiles() throws IOException {
    HoodieInstant instant1 = new HoodieInstant(State.REQUESTED, HoodieTimeline.COMMIT_ACTION, "1");
    HoodieInstant instant2 = new HoodieInstant(State.REQUESTED, HoodieTimeline.COMMIT_ACTION, "3");
    HoodieInstant instant3 = new HoodieInstant(State.REQUESTED, HoodieTimeline.COMMIT_ACTION, "5");
    HoodieInstant instant4 = new HoodieInstant(State.REQUESTED, HoodieTimeline.COMMIT_ACTION, "8");
    HoodieInstant instant1Complete = new HoodieInstant(false, HoodieTimeline.COMMIT_ACTION, "1");
    HoodieInstant instant2Complete = new HoodieInstant(false, HoodieTimeline.COMMIT_ACTION, "3");
    HoodieInstant instant3Complete = new HoodieInstant(false, HoodieTimeline.COMMIT_ACTION, "5");
    HoodieInstant instant4Complete = new HoodieInstant(false, HoodieTimeline.COMMIT_ACTION, "8");

    HoodieInstant instant5 = new HoodieInstant(true, HoodieTimeline.COMMIT_ACTION, "9");

    timeline = new HoodieActiveTimeline(metaClient);
    timeline.createNewInstant(instant1);
    timeline.transitionRequestedToInflight(instant1, Option.empty());
    timeline.saveAsComplete(new HoodieInstant(true, instant1.getAction(), instant1.getTimestamp()),
        Option.empty());
    timeline.createNewInstant(instant2);
    timeline.transitionRequestedToInflight(instant2, Option.empty());
    timeline.saveAsComplete(new HoodieInstant(true, instant2.getAction(), instant2.getTimestamp()),
        Option.empty());
    timeline.createNewInstant(instant3);
    timeline.transitionRequestedToInflight(instant3, Option.empty());
    timeline.saveAsComplete(new HoodieInstant(true, instant3.getAction(), instant3.getTimestamp()),
        Option.empty());
    timeline.createNewInstant(instant4);
    timeline.transitionRequestedToInflight(instant4, Option.empty());
    timeline.saveAsComplete(new HoodieInstant(true, instant4.getAction(), instant4.getTimestamp()),
        Option.empty());
    timeline.createNewInstant(instant5);
    timeline = timeline.reload();

    assertEquals(5, timeline.countInstants(), "Total instants should be 5");
    assertStreamEquals(
        Stream.of(instant1Complete, instant2Complete, instant3Complete, instant4Complete, instant5),
        timeline.getInstants(), "Check the instants stream");
    assertStreamEquals(
        Stream.of(instant1Complete, instant2Complete, instant3Complete, instant4Complete, instant5),
        timeline.getCommitTimeline().getInstants(), "Check the instants stream");
    assertStreamEquals(
        Stream.of(instant1Complete, instant2Complete, instant3Complete, instant4Complete),
        timeline.getCommitTimeline().filterCompletedInstants().getInstants(),
        "Check the instants stream");
    assertStreamEquals(Stream.of(instant5),
        timeline.getCommitTimeline().filterPendingExcludingCompaction().getInstants(),
        "Check the instants stream");

    // Backwards compatibility testing for reading compaction plans
    metaClient = HoodieTableMetaClient.withPropertyBuilder()
      .fromMetaClient(metaClient)
      .setTimelineLayoutVersion(VERSION_0)
      .initTable(metaClient.getHadoopConf(), metaClient.getBasePath());

    HoodieInstant instant6 = new HoodieInstant(State.REQUESTED, HoodieTimeline.COMPACTION_ACTION, "9");
    byte[] dummy = new byte[5];
    HoodieActiveTimeline oldTimeline = new HoodieActiveTimeline(
        HoodieTableMetaClient.builder().setConf(metaClient.getHadoopConf()).setBasePath(metaClient.getBasePath())
            .setLoadActiveTimelineOnLoad(true).setConsistencyGuardConfig(metaClient.getConsistencyGuardConfig())
            .setLayoutVersion(Option.of(new TimelineLayoutVersion(VERSION_0))).build());
    // Old Timeline writes both to aux and timeline folder
    oldTimeline.saveToCompactionRequested(instant6, Option.of(dummy));
    // Now use latest timeline version
    timeline = timeline.reload();
    // Ensure aux file is present
    assertTrue(metaClient.getFs().exists(new Path(metaClient.getMetaAuxiliaryPath(), instant6.getFileName())));
    // Read 5 bytes
    assertEquals(5, timeline.readCompactionPlanAsBytes(instant6).get().length);

    // Delete auxiliary file to mimic future release where we stop writing to aux
    metaClient.getFs().delete(new Path(metaClient.getMetaAuxiliaryPath(), instant6.getFileName()));

    // Ensure requested instant is not present in aux
    assertFalse(metaClient.getFs().exists(new Path(metaClient.getMetaAuxiliaryPath(), instant6.getFileName())));

    // Now read compaction plan again which should not throw exception
    assertEquals(5, timeline.readCompactionPlanAsBytes(instant6).get().length);
  }

  @Test
  public void testTimelineOperationsBasic() {
    timeline = new HoodieActiveTimeline(metaClient);
    assertTrue(timeline.empty());
    assertEquals(0, timeline.countInstants());
    assertEquals(Option.empty(), timeline.firstInstant());
    assertEquals(Option.empty(), timeline.nthInstant(5));
    assertEquals(Option.empty(), timeline.nthInstant(-1));
    assertEquals(Option.empty(), timeline.lastInstant());
    assertFalse(timeline.containsInstant(new HoodieInstant(false, HoodieTimeline.COMMIT_ACTION, "01")));
  }

  @Test
  public void testTimelineOperations() {
    timeline = new MockHoodieTimeline(Stream.of("01", "03", "05", "07", "09", "11", "13", "15", "17", "19"),
        Stream.of("21", "23"));
    assertStreamEquals(Stream.of("05", "07", "09", "11"),
        timeline.getCommitTimeline().filterCompletedInstants().findInstantsInRange("04", "11")
            .getInstants().map(HoodieInstant::getTimestamp),
        "findInstantsInRange should return 4 instants");
    assertStreamEquals(Stream.of("09", "11"),
        timeline.getCommitTimeline().filterCompletedInstants().findInstantsAfter("07", 2)
            .getInstants().map(HoodieInstant::getTimestamp),
        "findInstantsAfter 07 should return 2 instants");
    assertStreamEquals(Stream.of("01", "03", "05"),
        timeline.getCommitTimeline().filterCompletedInstants().findInstantsBefore("07")
            .getInstants().map(HoodieInstant::getTimestamp),
        "findInstantsBefore 07 should return 3 instants");
    assertFalse(timeline.empty());
    assertFalse(timeline.getCommitTimeline().filterPendingExcludingCompaction().empty());
    assertEquals(12, timeline.countInstants());
    HoodieTimeline activeCommitTimeline = timeline.getCommitTimeline().filterCompletedInstants();
    assertEquals(10, activeCommitTimeline.countInstants());

    assertEquals("01", activeCommitTimeline.firstInstant().get().getTimestamp());
    assertEquals("11", activeCommitTimeline.nthInstant(5).get().getTimestamp());
    assertEquals("19", activeCommitTimeline.lastInstant().get().getTimestamp());
    assertEquals("09", activeCommitTimeline.nthFromLastInstant(5).get().getTimestamp());
    assertTrue(activeCommitTimeline.containsInstant(new HoodieInstant(false, HoodieTimeline.COMMIT_ACTION, "09")));
    assertFalse(activeCommitTimeline.isBeforeTimelineStarts("02"));
    assertTrue(activeCommitTimeline.isBeforeTimelineStarts("00"));
  }

  @Test
  public void testTimelineGetOperations() {
    List<HoodieInstant> allInstants = getAllInstants();
    Supplier<Stream<HoodieInstant>> sup = allInstants::stream;
    timeline = new HoodieActiveTimeline(metaClient, true);
    timeline.setInstants(allInstants);

    /*
     * Helper function to check HoodieTimeline only contains some type of Instant actions.
     * @param timeline The HoodieTimeline to check
     * @param actions The actions that should be present in the timeline being checked
     */
    BiConsumer<HoodieTimeline, Set<String>> checkTimeline = (HoodieTimeline timeline, Set<String> actions) -> {
      sup.get().filter(i -> actions.contains(i.getAction())).forEach(i -> assertTrue(timeline.containsInstant(i)));
      sup.get().filter(i -> !actions.contains(i.getAction())).forEach(i -> assertFalse(timeline.containsInstant(i)));
    };

    // Test that various types of getXXX operations from HoodieActiveTimeline
    // return the correct set of Instant
    checkTimeline.accept(timeline.getCommitsTimeline(),
            CollectionUtils.createSet(HoodieTimeline.COMMIT_ACTION, HoodieTimeline.DELTA_COMMIT_ACTION, HoodieTimeline.REPLACE_COMMIT_ACTION));
    checkTimeline.accept(timeline.getWriteTimeline(),
            CollectionUtils.createSet(HoodieTimeline.COMMIT_ACTION, HoodieTimeline.DELTA_COMMIT_ACTION, HoodieTimeline.COMPACTION_ACTION, HoodieTimeline.REPLACE_COMMIT_ACTION));
    checkTimeline.accept(timeline.getCommitTimeline(),  CollectionUtils.createSet(HoodieTimeline.COMMIT_ACTION, HoodieTimeline.REPLACE_COMMIT_ACTION));
    checkTimeline.accept(timeline.getDeltaCommitTimeline(), Collections.singleton(HoodieTimeline.DELTA_COMMIT_ACTION));
    checkTimeline.accept(timeline.getCleanerTimeline(), Collections.singleton(HoodieTimeline.CLEAN_ACTION));
    checkTimeline.accept(timeline.getRollbackTimeline(), Collections.singleton(HoodieTimeline.ROLLBACK_ACTION));
    checkTimeline.accept(timeline.getRestoreTimeline(), Collections.singleton(HoodieTimeline.RESTORE_ACTION));
    checkTimeline.accept(timeline.getSavePointTimeline(), Collections.singleton(HoodieTimeline.SAVEPOINT_ACTION));
    checkTimeline.accept(timeline.getAllCommitsTimeline(),
            CollectionUtils.createSet(HoodieTimeline.COMMIT_ACTION, HoodieTimeline.DELTA_COMMIT_ACTION,
                    HoodieTimeline.CLEAN_ACTION, HoodieTimeline.COMPACTION_ACTION, HoodieTimeline.REPLACE_COMMIT_ACTION,
                    HoodieTimeline.SAVEPOINT_ACTION, HoodieTimeline.ROLLBACK_ACTION));

    // Get some random Instants
    Random rand = new Random();
    Set<String> randomInstants = sup.get().filter(i -> rand.nextBoolean())
                                          .map(HoodieInstant::getAction).collect(Collectors.toSet());
    checkTimeline.accept(timeline.getTimelineOfActions(randomInstants), randomInstants);
  }

  @Test
  public void testTimelineInstantOperations() {
    timeline = new HoodieActiveTimeline(metaClient, true);
    assertEquals(0, timeline.countInstants(), "No instant present");

    // revertToInflight
    HoodieInstant commit = new HoodieInstant(State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "1");
    timeline.createNewInstant(commit);
    timeline = timeline.reload();
    assertEquals(1, timeline.countInstants());
    assertTrue(timeline.containsInstant(commit));
    HoodieInstant inflight = timeline.revertToInflight(commit);
    // revert creates the .requested file
    timeline = timeline.reload();
    assertEquals(1, timeline.countInstants());
    assertTrue(timeline.containsInstant(inflight));
    assertFalse(timeline.containsInstant(commit));

    // deleteInflight
    timeline.deleteInflight(inflight);
    timeline = timeline.reload();
    assertEquals(1, timeline.countInstants());
    assertFalse(timeline.containsInstant(inflight));
    assertFalse(timeline.containsInstant(commit));

    // deletePending
    timeline.createNewInstant(commit);
    timeline.createNewInstant(inflight);
    timeline = timeline.reload();
    assertEquals(1, timeline.countInstants());
    timeline.deletePending(inflight);
    timeline = timeline.reload();
    assertEquals(1, timeline.countInstants());
    assertFalse(timeline.containsInstant(inflight));
    assertTrue(timeline.containsInstant(commit));

    // deleteCompactionRequested
    HoodieInstant compaction = new HoodieInstant(State.REQUESTED, HoodieTimeline.COMPACTION_ACTION, "2");
    timeline.createNewInstant(compaction);
    timeline = timeline.reload();
    assertEquals(2, timeline.countInstants());
    timeline.deleteCompactionRequested(compaction);
    timeline = timeline.reload();
    assertEquals(1, timeline.countInstants());
    assertFalse(timeline.containsInstant(inflight));
    assertFalse(timeline.containsInstant(compaction));
    assertTrue(timeline.containsInstant(commit));

    // transitionCompactionXXXtoYYY and revertCompactionXXXtoYYY
    compaction = new HoodieInstant(State.REQUESTED, HoodieTimeline.COMPACTION_ACTION, "3");
    timeline.createNewInstant(compaction);
    timeline = timeline.reload();
    assertTrue(timeline.containsInstant(compaction));
    inflight = timeline.transitionCompactionRequestedToInflight(compaction);
    timeline = timeline.reload();
    assertFalse(timeline.containsInstant(compaction));
    assertTrue(timeline.containsInstant(inflight));
    compaction = timeline.revertCompactionInflightToRequested(inflight);
    timeline = timeline.reload();
    assertTrue(timeline.containsInstant(compaction));
    assertFalse(timeline.containsInstant(inflight));
    inflight = timeline.transitionCompactionRequestedToInflight(compaction);
    compaction = timeline.transitionCompactionInflightToComplete(inflight, Option.empty());
    timeline = timeline.reload();
    assertTrue(timeline.containsInstant(compaction));
    assertFalse(timeline.containsInstant(inflight));

    // transitionCleanXXXtoYYY
    HoodieInstant clean = new HoodieInstant(State.REQUESTED, HoodieTimeline.CLEAN_ACTION, "4");
    timeline.saveToCleanRequested(clean, Option.empty());
    timeline = timeline.reload();
    assertTrue(timeline.containsInstant(clean));
    inflight = timeline.transitionCleanRequestedToInflight(clean, Option.empty());
    timeline = timeline.reload();
    assertFalse(timeline.containsInstant(clean));
    assertTrue(timeline.containsInstant(inflight));
    clean = timeline.transitionCleanInflightToComplete(inflight, Option.empty());
    timeline = timeline.reload();
    assertTrue(timeline.containsInstant(clean));
    assertFalse(timeline.containsInstant(inflight));

    // Various states of Instants
    HoodieInstant srcInstant = new HoodieInstant(State.COMPLETED, HoodieTimeline.RESTORE_ACTION, "5");
    HoodieInstant otherInstant = HoodieTimeline.getRequestedInstant(srcInstant);
    assertEquals(otherInstant, new HoodieInstant(State.REQUESTED, HoodieTimeline.RESTORE_ACTION, "5"));
    otherInstant = HoodieTimeline.getCleanRequestedInstant("5");
    assertEquals(otherInstant, new HoodieInstant(State.REQUESTED, HoodieTimeline.CLEAN_ACTION, "5"));
    otherInstant = HoodieTimeline.getCleanInflightInstant("5");
    assertEquals(otherInstant, new HoodieInstant(State.INFLIGHT, HoodieTimeline.CLEAN_ACTION, "5"));
    otherInstant = HoodieTimeline.getCompactionRequestedInstant("5");
    assertEquals(otherInstant, new HoodieInstant(State.REQUESTED, HoodieTimeline.COMPACTION_ACTION, "5"));
    otherInstant = HoodieTimeline.getCompactionInflightInstant("5");
    assertEquals(otherInstant, new HoodieInstant(State.INFLIGHT, HoodieTimeline.COMPACTION_ACTION, "5"));

    // containsOrBeforeTimelineStarts
    List<HoodieInstant> allInstants = getAllInstants();
    timeline = new HoodieActiveTimeline(metaClient, true);
    timeline.setInstants(allInstants);

    timeline.setInstants(allInstants);
    timeline.createNewInstant(new HoodieInstant(State.REQUESTED, HoodieTimeline.COMMIT_ACTION, "2"));
    allInstants.stream().map(HoodieInstant::getTimestamp).forEach(s -> assertTrue(timeline.containsOrBeforeTimelineStarts(s)));
    assertTrue(timeline.containsOrBeforeTimelineStarts("0"));
    assertFalse(timeline.containsOrBeforeTimelineStarts(String.valueOf(System.currentTimeMillis() + 1000)));
    assertFalse(timeline.getTimelineHash().isEmpty());
  }

  @Test
  public void testCreateInstants() {
    List<HoodieInstant> allInstants = getAllInstants();
    for (HoodieInstant instant : allInstants) {
      timeline.createNewInstant(instant);
    }

    timeline = timeline.reload();
    for (HoodieInstant instant : allInstants) {
      assertTrue(timeline.containsInstant(instant));
    }
  }

  @Test
  public void testInstantFilenameOperations() {
    HoodieInstant instantRequested = new HoodieInstant(State.REQUESTED, HoodieTimeline.RESTORE_ACTION, "5");
    HoodieInstant instantInflight = new HoodieInstant(State.INFLIGHT, HoodieTimeline.RESTORE_ACTION, "5");
    HoodieInstant instantComplete = new HoodieInstant(State.COMPLETED, HoodieTimeline.RESTORE_ACTION, "5");
    assertEquals(HoodieTimeline.getCommitFromCommitFile(instantRequested.getFileName()), "5");
    assertEquals(HoodieTimeline.getCommitFromCommitFile(instantInflight.getFileName()), "5");
    assertEquals(HoodieTimeline.getCommitFromCommitFile(instantComplete.getFileName()), "5");

    assertEquals(HoodieTimeline.makeFileNameAsComplete(instantInflight.getFileName()),
            instantComplete.getFileName());

    assertEquals(HoodieTimeline.makeFileNameAsInflight(instantComplete.getFileName()),
            instantInflight.getFileName());
  }

  @Test
  public void testFiltering() {
    List<HoodieInstant> allInstants = getAllInstants();
    Supplier<Stream<HoodieInstant>> sup = allInstants::stream;

    timeline = new HoodieActiveTimeline(metaClient);
    timeline.setInstants(allInstants);

    // getReverseOrderedInstants
    Stream<HoodieInstant> instants = timeline.getReverseOrderedInstants();
    List<HoodieInstant> v1 = instants.collect(Collectors.toList());
    List<HoodieInstant> v2 = sup.get().collect(Collectors.toList());
    Collections.reverse(v2);
    assertEquals(v1, v2);

    /*
     * Helper function to check HoodieTimeline only contains some type of Instant states.
     * @param timeline The HoodieTimeline to check
     * @param states The states that should be present in the timeline being checked
     */
    BiConsumer<HoodieTimeline, Set<State>> checkFilter = (HoodieTimeline timeline, Set<State> states) -> {
      sup.get().filter(i -> states.contains(i.getState())).forEach(i -> assertTrue(timeline.containsInstant(i)));
      sup.get().filter(i -> !states.contains(i.getState())).forEach(i -> assertFalse(timeline.containsInstant(i)));
    };

    checkFilter.accept(timeline.filter(i -> false), new HashSet<>());
    checkFilter.accept(timeline.filterInflights(), Collections.singleton(State.INFLIGHT));
    checkFilter.accept(timeline.filterInflightsAndRequested(),
            CollectionUtils.createSet(State.INFLIGHT, State.REQUESTED));

    // filterCompletedAndCompactionInstants
    // This cannot be done using checkFilter as it involves both states and actions
    final HoodieTimeline t1 = timeline.filterCompletedAndCompactionInstants();
    final Set<State> states = CollectionUtils.createSet(State.COMPLETED);
    final Set<String> actions = Collections.singleton(HoodieTimeline.COMPACTION_ACTION);
    sup.get().filter(i -> states.contains(i.getState()) || actions.contains(i.getAction()))
        .forEach(i -> assertTrue(t1.containsInstant(i)));
    sup.get().filter(i -> !(states.contains(i.getState()) || actions.contains(i.getAction())))
        .forEach(i -> assertFalse(t1.containsInstant(i)));

    // filterPendingCompactionTimeline
    final HoodieTimeline t2 = timeline.filterPendingCompactionTimeline();
    sup.get().filter(i -> i.getAction().equals(HoodieTimeline.COMPACTION_ACTION))
        .forEach(i -> assertTrue(t2.containsInstant(i)));
    sup.get().filter(i -> !i.getAction().equals(HoodieTimeline.COMPACTION_ACTION))
        .forEach(i -> assertFalse(t2.containsInstant(i)));
  }

  @Test
  public void testReplaceActionsTimeline() {
    int instantTime = 1;
    List<HoodieInstant> allInstants = new ArrayList<>();
    HoodieInstant instant = new HoodieInstant(State.COMPLETED, HoodieTimeline.COMMIT_ACTION, String.format("%03d", instantTime++));
    allInstants.add(instant);
    instant = new HoodieInstant(State.COMPLETED, HoodieTimeline.COMMIT_ACTION, String.format("%03d", instantTime++));
    allInstants.add(instant);
    instant = new HoodieInstant(State.COMPLETED, HoodieTimeline.REPLACE_COMMIT_ACTION, String.format("%03d", instantTime++));
    allInstants.add(instant);

    timeline = new HoodieActiveTimeline(metaClient);
    timeline.setInstants(allInstants);
    List<HoodieInstant> validReplaceInstants =
        timeline.getCompletedReplaceTimeline().getInstants().collect(Collectors.toList());

    assertEquals(1, validReplaceInstants.size());
    assertEquals(instant.getTimestamp(), validReplaceInstants.get(0).getTimestamp());
    assertEquals(HoodieTimeline.REPLACE_COMMIT_ACTION, validReplaceInstants.get(0).getAction());
  }

  @Test
  public void testCreateNewInstantTime() throws Exception {
    String lastInstantTime = HoodieActiveTimeline.createNewInstantTime();
    for (int i = 0; i < 3; ++i) {
      String newInstantTime = HoodieActiveTimeline.createNewInstantTime();
      assertTrue(HoodieTimeline.compareTimestamps(lastInstantTime, HoodieTimeline.LESSER_THAN, newInstantTime));
      lastInstantTime = newInstantTime;
    }

    // All zero timestamp can be parsed
    HoodieActiveTimeline.parseInstantTime("00000000000000");

    // Multiple thread test
    final int numChecks = 100000;
    final int numThreads = 100;
    final long milliSecondsInYear = 365 * 24 * 3600 * 1000;
    ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
    List<Future> futures = new ArrayList<>(numThreads);
    for (int idx = 0; idx < numThreads; ++idx) {
      futures.add(executorService.submit(() -> {
        Date date = new Date(System.currentTimeMillis() + (int)(Math.random() * numThreads) * milliSecondsInYear);
        final String expectedFormat = HoodieActiveTimeline.formatInstantTime(date);
        for (int tidx = 0; tidx < numChecks; ++tidx) {
          final String curFormat = HoodieActiveTimeline.formatInstantTime(date);
          if (!curFormat.equals(expectedFormat)) {
            throw new HoodieException("Format error: expected=" + expectedFormat + ", curFormat=" + curFormat);
          }
        }
      }));
    }

    executorService.shutdown();
    assertTrue(executorService.awaitTermination(10, TimeUnit.SECONDS));
    // required to catch exceptions
    for (Future f : futures) {
      f.get();
    }
  }

  /**
   * Returns an exhaustive list of all possible HoodieInstant.
   * @return list of HoodieInstant
   */
  private List<HoodieInstant> getAllInstants() {
    timeline = new HoodieActiveTimeline(metaClient);
    List<HoodieInstant> allInstants = new ArrayList<>();
    long instantTime = 1;
    for (State state : State.values()) {
      if (state == State.INVALID) {
        continue;
      }
      for (String action : HoodieTimeline.VALID_ACTIONS_IN_TIMELINE) {
        // Following are not valid combinations of actions and state so we should
        // not be generating them.
        if (state == State.REQUESTED) {
          if (action.equals(HoodieTimeline.SAVEPOINT_ACTION) || action.equals(HoodieTimeline.RESTORE_ACTION)
              || action.equals(HoodieTimeline.ROLLBACK_ACTION)) {
            continue;
          }
        }
        if (state == State.INFLIGHT && action.equals(HoodieTimeline.ROLLBACK_ACTION)) {
          continue;
        }
        if (state == State.COMPLETED && action.equals(HoodieTimeline.ROLLBACK_ACTION)) {
          continue;
        }
        // Compaction complete is called commit complete
        if (state == State.COMPLETED && action.equals(HoodieTimeline.COMPACTION_ACTION)) {
          action = HoodieTimeline.COMMIT_ACTION;
        }

        allInstants.add(new HoodieInstant(state, action, String.format("%03d", instantTime++)));
      }
    }
    return allInstants;
  }
}
