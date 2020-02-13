/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.server.api.resources;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import org.apache.pinot.core.data.manager.InstanceDataManager;
import org.apache.pinot.core.data.manager.SegmentDataManager;
import org.apache.pinot.core.data.manager.TableDataManager;
import org.apache.pinot.server.starter.ServerInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Api(tags = "UpsertDebug")
@Path("/")
public class UpsertDebugResource {
  private static final Logger LOGGER = LoggerFactory.getLogger(UpsertDebugResource.class);

  @Inject
  ServerInstance serverInstance;

  @GET
  @Produces(MediaType.TEXT_PLAIN)
  @Path("/upsert/{tableName}/{segmentName}/{offset}")
  @ApiOperation(value = "$validFrom and $validUntil value", notes = "")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Success", response = String.class),
      @ApiResponse(code = 500, message = "Internal server error"),
  })
  public String getUpsertDataAtOffset(
      @ApiParam(value = "Table name including type", required = true, example = "myTable_REALTIME") @PathParam("tableName") String tableName,
      @ApiParam(value = "segment name", required = true, example = "eats_supply_update__0__0__20190923T0700Z") @PathParam("segmentName") String segmentName,
      @ApiParam(value = "offset", required = true, example = "100") @PathParam("offset") String offsetStr
  ) {
    if (!serverInstance.isUpsertEnabled()) {
      return "not an upsert server";
    }
    InstanceDataManager instanceDataManager = serverInstance.getInstanceDataManager();
    TableDataManager tableDataManager = instanceDataManager.getTableDataManager(tableName);
    if (tableDataManager == null) {
      return "no table for " + tableName;
    }
    /*
    SegmentDataManager segmentDataManager = null;
    try {
      segmentDataManager = tableDataManager.acquireSegment(segmentName);
      if (segmentDataManager == null) {
        return "cannot find associate segment for segment " + segmentName;
      }
      if (!(segmentDataManager instanceof UpsertSegmentDataManager)) {
        return "it is not an upsert table";
      } else {
        long offset = Long.parseLong(offsetStr);
        LOGGER.info("getting virtual column for table {} segment {} offset {}", tableName, segmentName, offset);
        return ( segmentDataManager).getVirtualColumnInfo(Long.parseLong(offsetStr));
      }
    } catch (Exception ex) {
      LOGGER.error("failed to fetch virtual column info", ex);
      throw new WebApplicationException("Failed to fetch virtual column info" + ex.getMessage(),
          Response.Status.INTERNAL_SERVER_ERROR);
    } finally {
      if (segmentDataManager != null) {
        tableDataManager.releaseSegment(segmentDataManager);
      }
    }
    */
    return "";
  }
}
