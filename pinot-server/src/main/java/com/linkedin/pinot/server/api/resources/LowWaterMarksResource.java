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
package com.linkedin.pinot.server.api.resources;

import org.apache.pinot.common.restlet.resources.ResourceUtils;
import com.linkedin.pinot.common.restlet.resources.TableLowWaterMarksInfo;
import org.apache.pinot.core.data.manager.InstanceDataManager;
import org.apache.pinot.server.starter.ServerInstance;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Api(tags = "LowWaterMarks")
@Path("/")
public class LowWaterMarksResource {

    @Inject
    ServerInstance serverInstance;

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/lwms")
    @ApiOperation(value = "Show the lwms of tables ", notes = "Returns the lwms of all upsert enable tables in this server")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Success"),
            @ApiResponse(code = 500, message = "Internal server error"),
    })
    public String getLowWaterMarks() {
        InstanceDataManager instanceDataManager = serverInstance.getInstanceDataManager();

        if (instanceDataManager == null) {
            throw new WebApplicationException("Invalid server initialization", Response.Status.INTERNAL_SERVER_ERROR);
        }
        return ResourceUtils.convertToJsonString(new TableLowWaterMarksInfo(instanceDataManager.getLowWaterMarks()));
    }
}
