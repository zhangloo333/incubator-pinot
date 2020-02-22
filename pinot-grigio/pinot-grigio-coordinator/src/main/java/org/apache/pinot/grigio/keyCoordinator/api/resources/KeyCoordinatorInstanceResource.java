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
package org.apache.pinot.grigio.keyCoordinator.api.resources;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import org.apache.pinot.grigio.keyCoordinator.api.KeyCoordinatorInstance;
import org.apache.pinot.grigio.keyCoordinator.helix.KeyCoordinatorClusterHelixManager;
import org.apache.pinot.spi.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.util.List;


@Api(tags = "Instance")
@Path("/")
public class KeyCoordinatorInstanceResource {

  private static final Logger LOGGER = LoggerFactory.getLogger(KeyCoordinatorInstanceResource.class);

  @Inject
  private KeyCoordinatorClusterHelixManager _keyCoordinatorClusterHelixManager;

  @GET
  @Path("/instances")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "List all instances", produces = MediaType.APPLICATION_JSON)
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Success"),
      @ApiResponse(code = 500, message = "Internal error")
  })
  public String getAllInstances() {
    LOGGER.info("Trying to get all key coordinator instances");
    List<String> instances = _keyCoordinatorClusterHelixManager.getAllInstances();
    return JsonUtils.objectToJsonNode(instances).toString();
  }

  @POST
  @Path("/instances")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.TEXT_PLAIN)
  @ApiOperation(value = "Create a new instance", consumes = MediaType.APPLICATION_JSON, produces = MediaType.TEXT_PLAIN,
      notes = "Create a new instance with given instance config")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Success"),
      @ApiResponse(code = 409, message = "Instance already exists"),
      @ApiResponse(code = 500, message = "Internal error")
  })
  public String addInstance(KeyCoordinatorInstance keyCoordinatorInstance) {
    LOGGER.info("Trying to add a new key coordinator instance");
    _keyCoordinatorClusterHelixManager.addInstance(keyCoordinatorInstance);
    return "Successfully created instance";
  }

  @DELETE
  @Path("/instances")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.TEXT_PLAIN)
  @ApiOperation(value = "Drop an instance", consumes = MediaType.APPLICATION_JSON, produces = MediaType.TEXT_PLAIN,
      notes = "Drop an instance")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Success"),
      @ApiResponse(code = 404, message = "Instance not found"),
      @ApiResponse(code = 409, message = "Instance cannot be dropped"),
      @ApiResponse(code = 500, message = "Internal error")
  })
  public String dropInstance(KeyCoordinatorInstance keyCoordinatorInstance) {
    LOGGER.info("Trying to drop a key coordinator instance");
    _keyCoordinatorClusterHelixManager.dropInstance(keyCoordinatorInstance);
    return "Successfully dropped instance";
  }
}

