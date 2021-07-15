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

package org.apache.hudi.timeline.service;

import org.apache.hudi.common.metrics.Registry;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.dto.BaseFileDTO;
import org.apache.hudi.common.table.timeline.dto.ClusteringOpDTO;
import org.apache.hudi.common.table.timeline.dto.CompactionOpDTO;
import org.apache.hudi.common.table.timeline.dto.FileGroupDTO;
import org.apache.hudi.common.table.timeline.dto.FileSliceDTO;
import org.apache.hudi.common.table.timeline.dto.InstantDTO;
import org.apache.hudi.common.table.timeline.dto.TimelineDTO;
import org.apache.hudi.common.table.view.FileSystemViewManager;
import org.apache.hudi.common.table.view.RemoteHoodieTableFileSystemView;
import org.apache.hudi.common.table.view.SyncableFileSystemView;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.timeline.service.handlers.BaseFileHandler;
import org.apache.hudi.timeline.service.handlers.FileSliceHandler;
import org.apache.hudi.timeline.service.handlers.TimelineHandler;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.javalin.Context;
import io.javalin.Handler;
import io.javalin.Javalin;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

/**
 *  Main REST Handler class that handles and delegates calls to timeline relevant handlers.
 */
public class RequestHandler {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final Logger LOG = LogManager.getLogger(RequestHandler.class);

  private final FileSystemViewManager viewManager;
  private final Javalin app;
  private final TimelineHandler instantHandler;
  private final FileSliceHandler sliceHandler;
  private final BaseFileHandler dataFileHandler;
  private Registry metricsRegistry = Registry.getRegistry("TimelineService");
  private ScheduledExecutorService asyncResultService = Executors.newSingleThreadScheduledExecutor();
  private final boolean useAsync;

  public RequestHandler(Javalin app, Configuration conf, FileSystemViewManager viewManager, boolean useAsync) throws IOException {
    this.viewManager = viewManager;
    this.app = app;
    this.instantHandler = new TimelineHandler(conf, viewManager);
    this.sliceHandler = new FileSliceHandler(conf, viewManager);
    this.dataFileHandler = new BaseFileHandler(conf, viewManager);
    this.useAsync = useAsync;
    if (useAsync) {
      asyncResultService = Executors.newSingleThreadScheduledExecutor();
    }
  }

  public RequestHandler(Javalin app, Configuration conf, FileSystemViewManager viewManager) throws IOException {
    this(app, conf, viewManager, false);
  }

  public void register() {
    registerDataFilesAPI();
    registerFileSlicesAPI();
    registerTimelineAPI();
  }

  /**
   * Determines if local view of table's timeline is behind that of client's view.
   */
  private boolean isLocalViewBehind(Context ctx) {
    String basePath = ctx.queryParam(RemoteHoodieTableFileSystemView.BASEPATH_PARAM);
    String lastKnownInstantFromClient =
        ctx.queryParam(RemoteHoodieTableFileSystemView.LAST_INSTANT_TS, HoodieTimeline.INVALID_INSTANT_TS);
    String timelineHashFromClient = ctx.queryParam(RemoteHoodieTableFileSystemView.TIMELINE_HASH, "");
    HoodieTimeline localTimeline =
        viewManager.getFileSystemView(basePath).getTimeline().filterCompletedAndCompactionInstants();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Client [ LastTs=" + lastKnownInstantFromClient + ", TimelineHash=" + timelineHashFromClient
          + "], localTimeline=" + localTimeline.getInstants().collect(Collectors.toList()));
    }

    if ((localTimeline.getInstants().count() == 0)
        && HoodieTimeline.INVALID_INSTANT_TS.equals(lastKnownInstantFromClient)) {
      return false;
    }

    String localTimelineHash = localTimeline.getTimelineHash();
    if (!localTimelineHash.equals(timelineHashFromClient)) {
      return true;
    }

    // As a safety check, even if hash is same, ensure instant is present
    return !localTimeline.containsOrBeforeTimelineStarts(lastKnownInstantFromClient);
  }

  /**
   * Syncs data-set view if local view is behind.
   */
  private boolean syncIfLocalViewBehind(Context ctx) {
    if (isLocalViewBehind(ctx)) {
      String basePath = ctx.queryParam(RemoteHoodieTableFileSystemView.BASEPATH_PARAM);
      String lastKnownInstantFromClient =
          ctx.queryParam(RemoteHoodieTableFileSystemView.LAST_INSTANT_TS, HoodieTimeline.INVALID_INSTANT_TS);
      SyncableFileSystemView view = viewManager.getFileSystemView(basePath);
      if (isLocalViewBehind(ctx)) {
        HoodieTimeline localTimeline = viewManager.getFileSystemView(basePath).getTimeline();
        LOG.info("Syncing view as client passed last known instant " + lastKnownInstantFromClient
            + " as last known instant but server has the following last instant on timeline :"
            + localTimeline.lastInstant());
        view.sync();
        return true;
      }
    }
    return false;
  }

  private void writeValueAsString(Context ctx, Object obj) throws JsonProcessingException {
    if (useAsync) {
      writeValueAsStringAsync(ctx, obj);
    } else {
      writeValueAsStringSync(ctx, obj);
    }
  }

  private void writeValueAsStringSync(Context ctx, Object obj) throws JsonProcessingException {
    HoodieTimer timer = new HoodieTimer().startTimer();
    boolean prettyPrint = ctx.queryParam("pretty") != null;
    String result =
        prettyPrint ? OBJECT_MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(obj) : OBJECT_MAPPER.writeValueAsString(obj);
    final long jsonifyTime = timer.endTimer();
    ctx.result(result);
    metricsRegistry.add("WRITE_VALUE_CNT", 1);
    metricsRegistry.add("WRITE_VALUE_TIME", jsonifyTime);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Jsonify TimeTaken=" + jsonifyTime);
    }
  }

  private void writeValueAsStringAsync(Context ctx, Object obj) throws JsonProcessingException {
    ctx.result(CompletableFuture.supplyAsync(() -> {
      HoodieTimer timer = new HoodieTimer().startTimer();
      boolean prettyPrint = ctx.queryParam("pretty") != null;
      try {
        String result = prettyPrint ? OBJECT_MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(obj) : OBJECT_MAPPER.writeValueAsString(obj);
        final long jsonifyTime = timer.endTimer();
        metricsRegistry.add("WRITE_VALUE_CNT", 1);
        metricsRegistry.add("WRITE_VALUE_TIME", jsonifyTime);
        if (LOG.isDebugEnabled()) {
          LOG.debug("Jsonify TimeTaken=" + jsonifyTime);
        }
        return result;
      } catch (JsonProcessingException e) {
        throw new HoodieException("Failed to JSON encode the value", e);
      }
    }, asyncResultService));
  }

  /**
   * Register Timeline API calls.
   */
  private void registerTimelineAPI() {
    app.get(RemoteHoodieTableFileSystemView.LAST_INSTANT, new ViewHandler(ctx -> {
      metricsRegistry.add("LAST_INSTANT", 1);
      List<InstantDTO> dtos = instantHandler
          .getLastInstant(ctx.validatedQueryParam(RemoteHoodieTableFileSystemView.BASEPATH_PARAM).getValue());
      writeValueAsString(ctx, dtos);
    }, false));

    app.get(RemoteHoodieTableFileSystemView.TIMELINE, new ViewHandler(ctx -> {
      metricsRegistry.add("TIMELINE", 1);
      TimelineDTO dto = instantHandler
          .getTimeline(ctx.validatedQueryParam(RemoteHoodieTableFileSystemView.BASEPATH_PARAM).getValue());
      writeValueAsString(ctx, dto);
    }, false));
  }

  /**
   * Register Data-Files API calls.
   */
  private void registerDataFilesAPI() {
    app.get(RemoteHoodieTableFileSystemView.LATEST_PARTITION_DATA_FILES_URL, new ViewHandler(ctx -> {
      metricsRegistry.add("LATEST_PARTITION_DATA_FILES", 1);
      List<BaseFileDTO> dtos = dataFileHandler.getLatestDataFiles(
          ctx.validatedQueryParam(RemoteHoodieTableFileSystemView.BASEPATH_PARAM).getOrThrow(),
          ctx.queryParam(RemoteHoodieTableFileSystemView.PARTITION_PARAM,""));
      writeValueAsString(ctx, dtos);
    }, true));

    app.get(RemoteHoodieTableFileSystemView.LATEST_PARTITION_DATA_FILE_URL, new ViewHandler(ctx -> {
      metricsRegistry.add("LATEST_PARTITION_DATA_FILE", 1);
      List<BaseFileDTO> dtos = dataFileHandler.getLatestDataFile(
          ctx.validatedQueryParam(RemoteHoodieTableFileSystemView.BASEPATH_PARAM).getOrThrow(),
          ctx.queryParam(RemoteHoodieTableFileSystemView.PARTITION_PARAM,""),
          ctx.validatedQueryParam(RemoteHoodieTableFileSystemView.FILEID_PARAM).getOrThrow());
      writeValueAsString(ctx, dtos);
    }, true));

    app.get(RemoteHoodieTableFileSystemView.LATEST_ALL_DATA_FILES, new ViewHandler(ctx -> {
      metricsRegistry.add("LATEST_ALL_DATA_FILES", 1);
      List<BaseFileDTO> dtos = dataFileHandler
          .getLatestDataFiles(ctx.validatedQueryParam(RemoteHoodieTableFileSystemView.BASEPATH_PARAM).getOrThrow());
      writeValueAsString(ctx, dtos);
    }, true));

    app.get(RemoteHoodieTableFileSystemView.LATEST_DATA_FILES_BEFORE_ON_INSTANT_URL, new ViewHandler(ctx -> {
      metricsRegistry.add("LATEST_DATA_FILES_BEFORE_ON_INSTANT", 1);
      List<BaseFileDTO> dtos = dataFileHandler.getLatestDataFilesBeforeOrOn(
          ctx.validatedQueryParam(RemoteHoodieTableFileSystemView.BASEPATH_PARAM).getOrThrow(),
          ctx.queryParam(RemoteHoodieTableFileSystemView.PARTITION_PARAM,""),
          ctx.validatedQueryParam(RemoteHoodieTableFileSystemView.MAX_INSTANT_PARAM).getOrThrow());
      writeValueAsString(ctx, dtos);
    }, true));

    app.get(RemoteHoodieTableFileSystemView.LATEST_DATA_FILE_ON_INSTANT_URL, new ViewHandler(ctx -> {
      metricsRegistry.add("LATEST_DATA_FILE_ON_INSTANT", 1);
      List<BaseFileDTO> dtos = dataFileHandler.getLatestDataFileOn(
          ctx.validatedQueryParam(RemoteHoodieTableFileSystemView.BASEPATH_PARAM).getOrThrow(),
          ctx.queryParam(RemoteHoodieTableFileSystemView.PARTITION_PARAM,""),
          ctx.queryParam(RemoteHoodieTableFileSystemView.INSTANT_PARAM),
          ctx.validatedQueryParam(RemoteHoodieTableFileSystemView.FILEID_PARAM).getOrThrow());
      writeValueAsString(ctx, dtos);
    }, true));

    app.get(RemoteHoodieTableFileSystemView.ALL_DATA_FILES, new ViewHandler(ctx -> {
      metricsRegistry.add("ALL_DATA_FILES", 1);
      List<BaseFileDTO> dtos = dataFileHandler.getAllDataFiles(
          ctx.validatedQueryParam(RemoteHoodieTableFileSystemView.BASEPATH_PARAM).getOrThrow(),
          ctx.queryParam(RemoteHoodieTableFileSystemView.PARTITION_PARAM,""));
      writeValueAsString(ctx, dtos);
    }, true));

    app.get(RemoteHoodieTableFileSystemView.LATEST_DATA_FILES_RANGE_INSTANT_URL, new ViewHandler(ctx -> {
      metricsRegistry.add("LATEST_DATA_FILES_RANGE_INSTANT", 1);
      List<BaseFileDTO> dtos = dataFileHandler.getLatestDataFilesInRange(
          ctx.validatedQueryParam(RemoteHoodieTableFileSystemView.BASEPATH_PARAM).getOrThrow(), Arrays
              .asList(ctx.validatedQueryParam(RemoteHoodieTableFileSystemView.INSTANTS_PARAM).getOrThrow().split(",")));
      writeValueAsString(ctx, dtos);
    }, true));
  }

  /**
   * Register File Slices API calls.
   */
  private void registerFileSlicesAPI() {
    app.get(RemoteHoodieTableFileSystemView.LATEST_PARTITION_SLICES_URL, new ViewHandler(ctx -> {
      metricsRegistry.add("LATEST_PARTITION_SLICES", 1);
      List<FileSliceDTO> dtos = sliceHandler.getLatestFileSlices(
          ctx.validatedQueryParam(RemoteHoodieTableFileSystemView.BASEPATH_PARAM).getOrThrow(),
          ctx.queryParam(RemoteHoodieTableFileSystemView.PARTITION_PARAM,""));
      writeValueAsString(ctx, dtos);
    }, true));

    app.get(RemoteHoodieTableFileSystemView.LATEST_PARTITION_SLICE_URL, new ViewHandler(ctx -> {
      metricsRegistry.add("LATEST_PARTITION_SLICE", 1);
      List<FileSliceDTO> dtos = sliceHandler.getLatestFileSlice(
          ctx.validatedQueryParam(RemoteHoodieTableFileSystemView.BASEPATH_PARAM).getOrThrow(),
          ctx.queryParam(RemoteHoodieTableFileSystemView.PARTITION_PARAM,""),
          ctx.validatedQueryParam(RemoteHoodieTableFileSystemView.FILEID_PARAM).getOrThrow());
      writeValueAsString(ctx, dtos);
    }, true));

    app.get(RemoteHoodieTableFileSystemView.LATEST_PARTITION_UNCOMPACTED_SLICES_URL, new ViewHandler(ctx -> {
      metricsRegistry.add("LATEST_PARTITION_UNCOMPACTED_SLICES", 1);
      List<FileSliceDTO> dtos = sliceHandler.getLatestUnCompactedFileSlices(
          ctx.validatedQueryParam(RemoteHoodieTableFileSystemView.BASEPATH_PARAM).getOrThrow(),
          ctx.queryParam(RemoteHoodieTableFileSystemView.PARTITION_PARAM,""));
      writeValueAsString(ctx, dtos);
    }, true));

    app.get(RemoteHoodieTableFileSystemView.ALL_SLICES_URL, new ViewHandler(ctx -> {
      metricsRegistry.add("ALL_SLICES", 1);
      List<FileSliceDTO> dtos = sliceHandler.getAllFileSlices(
          ctx.validatedQueryParam(RemoteHoodieTableFileSystemView.BASEPATH_PARAM).getOrThrow(),
          ctx.queryParam(RemoteHoodieTableFileSystemView.PARTITION_PARAM,""));
      writeValueAsString(ctx, dtos);
    }, true));

    app.get(RemoteHoodieTableFileSystemView.LATEST_SLICES_RANGE_INSTANT_URL, new ViewHandler(ctx -> {
      metricsRegistry.add("LATEST_SLICE_RANGE_INSTANT", 1);
      List<FileSliceDTO> dtos = sliceHandler.getLatestFileSliceInRange(
          ctx.validatedQueryParam(RemoteHoodieTableFileSystemView.BASEPATH_PARAM).getOrThrow(), Arrays
              .asList(ctx.validatedQueryParam(RemoteHoodieTableFileSystemView.INSTANTS_PARAM).getOrThrow().split(",")));
      writeValueAsString(ctx, dtos);
    }, true));

    app.get(RemoteHoodieTableFileSystemView.LATEST_SLICES_MERGED_BEFORE_ON_INSTANT_URL, new ViewHandler(ctx -> {
      metricsRegistry.add("LATEST_SLICES_MERGED_BEFORE_ON_INSTANT", 1);
      List<FileSliceDTO> dtos = sliceHandler.getLatestMergedFileSlicesBeforeOrOn(
          ctx.validatedQueryParam(RemoteHoodieTableFileSystemView.BASEPATH_PARAM).getOrThrow(),
          ctx.queryParam(RemoteHoodieTableFileSystemView.PARTITION_PARAM,""),
          ctx.validatedQueryParam(RemoteHoodieTableFileSystemView.MAX_INSTANT_PARAM).getOrThrow());
      writeValueAsString(ctx, dtos);
    }, true));

    app.get(RemoteHoodieTableFileSystemView.LATEST_SLICES_BEFORE_ON_INSTANT_URL, new ViewHandler(ctx -> {
      metricsRegistry.add("LATEST_SLICES_BEFORE_ON_INSTANT", 1);
      List<FileSliceDTO> dtos = sliceHandler.getLatestFileSlicesBeforeOrOn(
          ctx.validatedQueryParam(RemoteHoodieTableFileSystemView.BASEPATH_PARAM).getOrThrow(),
          ctx.queryParam(RemoteHoodieTableFileSystemView.PARTITION_PARAM,""),
          ctx.validatedQueryParam(RemoteHoodieTableFileSystemView.MAX_INSTANT_PARAM).getOrThrow(),
          Boolean.parseBoolean(
              ctx.validatedQueryParam(RemoteHoodieTableFileSystemView.INCLUDE_FILES_IN_PENDING_COMPACTION_PARAM)
                  .getOrThrow()));
      writeValueAsString(ctx, dtos);
    }, true));

    app.get(RemoteHoodieTableFileSystemView.PENDING_COMPACTION_OPS, new ViewHandler(ctx -> {
      metricsRegistry.add("PEDING_COMPACTION_OPS", 1);
      List<CompactionOpDTO> dtos = sliceHandler.getPendingCompactionOperations(
          ctx.validatedQueryParam(RemoteHoodieTableFileSystemView.BASEPATH_PARAM).getOrThrow());
      writeValueAsString(ctx, dtos);
    }, true));

    app.get(RemoteHoodieTableFileSystemView.ALL_FILEGROUPS_FOR_PARTITION_URL, new ViewHandler(ctx -> {
      metricsRegistry.add("ALL_FILEGROUPS_FOR_PARTITION", 1);
      List<FileGroupDTO> dtos = sliceHandler.getAllFileGroups(
          ctx.validatedQueryParam(RemoteHoodieTableFileSystemView.BASEPATH_PARAM).getOrThrow(),
          ctx.queryParam(RemoteHoodieTableFileSystemView.PARTITION_PARAM,""));
      writeValueAsString(ctx, dtos);
    }, true));

    app.post(RemoteHoodieTableFileSystemView.REFRESH_TABLE, new ViewHandler(ctx -> {
      metricsRegistry.add("REFRESH_TABLE", 1);
      boolean success = sliceHandler
          .refreshTable(ctx.validatedQueryParam(RemoteHoodieTableFileSystemView.BASEPATH_PARAM).getOrThrow());
      writeValueAsString(ctx, success);
    }, false));

    app.get(RemoteHoodieTableFileSystemView.ALL_REPLACED_FILEGROUPS_BEFORE_OR_ON, new ViewHandler(ctx -> {
      metricsRegistry.add("ALL_REPLACED_FILEGROUPS_BEFORE_OR_ON", 1);
      List<FileGroupDTO> dtos = sliceHandler.getReplacedFileGroupsBeforeOrOn(
          ctx.validatedQueryParam(RemoteHoodieTableFileSystemView.BASEPATH_PARAM).getOrThrow(),
          ctx.queryParam(RemoteHoodieTableFileSystemView.MAX_INSTANT_PARAM,""),
          ctx.queryParam(RemoteHoodieTableFileSystemView.PARTITION_PARAM,""));
      writeValueAsString(ctx, dtos);
    }, true));

    app.get(RemoteHoodieTableFileSystemView.ALL_REPLACED_FILEGROUPS_BEFORE, new ViewHandler(ctx -> {
      metricsRegistry.add("ALL_REPLACED_FILEGROUPS_BEFORE", 1);
      List<FileGroupDTO> dtos = sliceHandler.getReplacedFileGroupsBefore(
          ctx.validatedQueryParam(RemoteHoodieTableFileSystemView.BASEPATH_PARAM).getOrThrow(),
          ctx.queryParam(RemoteHoodieTableFileSystemView.MAX_INSTANT_PARAM,""),
          ctx.queryParam(RemoteHoodieTableFileSystemView.PARTITION_PARAM,""));
      writeValueAsString(ctx, dtos);
    }, true));

    app.get(RemoteHoodieTableFileSystemView.ALL_REPLACED_FILEGROUPS_PARTITION, new ViewHandler(ctx -> {
      metricsRegistry.add("ALL_REPLACED_FILEGROUPS_PARTITION", 1);
      List<FileGroupDTO> dtos = sliceHandler.getAllReplacedFileGroups(
          ctx.validatedQueryParam(RemoteHoodieTableFileSystemView.BASEPATH_PARAM).getOrThrow(),
          ctx.queryParam(RemoteHoodieTableFileSystemView.PARTITION_PARAM,""));
      writeValueAsString(ctx, dtos);
    }, true));

    app.get(RemoteHoodieTableFileSystemView.PENDING_CLUSTERING_FILEGROUPS, new ViewHandler(ctx -> {
      metricsRegistry.add("PENDING_CLUSTERING_FILEGROUPS", 1);
      List<ClusteringOpDTO> dtos = sliceHandler.getFileGroupsInPendingClustering(
          ctx.validatedQueryParam(RemoteHoodieTableFileSystemView.BASEPATH_PARAM).getOrThrow());
      writeValueAsString(ctx, dtos);
    }, true));
  }

  private static boolean isRefreshCheckDisabledInQuery(Context ctxt) {
    return Boolean.parseBoolean(ctxt.queryParam(RemoteHoodieTableFileSystemView.REFRESH_OFF));
  }

  /**
   * Used for logging and performing refresh check.
   */
  private class ViewHandler implements Handler {

    private final Handler handler;
    private final boolean performRefreshCheck;

    ViewHandler(Handler handler, boolean performRefreshCheck) {
      this.handler = handler;
      this.performRefreshCheck = performRefreshCheck;
    }

    @Override
    public void handle(@NotNull Context context) throws Exception {
      boolean success = true;
      long beginTs = System.currentTimeMillis();
      boolean synced = false;
      boolean refreshCheck = performRefreshCheck && !isRefreshCheckDisabledInQuery(context);
      long refreshCheckTimeTaken = 0;
      long handleTimeTaken = 0;
      long finalCheckTimeTaken = 0;
      try {
        if (refreshCheck) {
          long beginRefreshCheck = System.currentTimeMillis();
          synced = syncIfLocalViewBehind(context);
          long endRefreshCheck = System.currentTimeMillis();
          refreshCheckTimeTaken = endRefreshCheck - beginRefreshCheck;
        }

        long handleBeginMs = System.currentTimeMillis();
        handler.handle(context);
        long handleEndMs = System.currentTimeMillis();
        handleTimeTaken = handleEndMs - handleBeginMs;

        if (refreshCheck) {
          long beginFinalCheck = System.currentTimeMillis();
          String errMsg =
              "Last known instant from client was "
                  + context.queryParam(RemoteHoodieTableFileSystemView.LAST_INSTANT_TS,
                      HoodieTimeline.INVALID_INSTANT_TS)
                  + " but server has the following timeline "
                  + viewManager.getFileSystemView(context.queryParam(RemoteHoodieTableFileSystemView.BASEPATH_PARAM))
                      .getTimeline().getInstants().collect(Collectors.toList());
          ValidationUtils.checkArgument(!isLocalViewBehind(context), errMsg);
          long endFinalCheck = System.currentTimeMillis();
          finalCheckTimeTaken = endFinalCheck - beginFinalCheck;
        }
      } catch (RuntimeException re) {
        success = false;
        LOG.error("Got runtime exception servicing request " + context.queryString(), re);
        throw re;
      } finally {
        long endTs = System.currentTimeMillis();
        long timeTakenMillis = endTs - beginTs;
        metricsRegistry.add("TOTAL_API_TIME", timeTakenMillis);
        metricsRegistry.add("TOTAL_REFRESH_TIME", refreshCheckTimeTaken);
        metricsRegistry.add("TOTAL_HANDLE_TIME", handleTimeTaken);
        metricsRegistry.add("TOTAL_CHECK_TIME", finalCheckTimeTaken);
        metricsRegistry.add("TOTAL_API_CALLS", 1);

        LOG.debug(String.format(
                "TimeTakenMillis[Total=%d, Refresh=%d, handle=%d, Check=%d], "
                    + "Success=%s, Query=%s, Host=%s, synced=%s",
                timeTakenMillis, refreshCheckTimeTaken, handleTimeTaken, finalCheckTimeTaken, success,
                context.queryString(), context.host(), synced));
      }
    }
  }
}
