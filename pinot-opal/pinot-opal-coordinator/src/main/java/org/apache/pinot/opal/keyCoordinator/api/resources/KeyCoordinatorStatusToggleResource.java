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
package org.apache.pinot.opal.keyCoordinator.api.resources;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import javax.inject.Inject;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import org.apache.pinot.opal.keyCoordinator.helix.KeyCoordinatorClusterHelixManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * APIs to start/stop consuming key coordinator messages after initialization of key coordinator cluster
 */
@Api(tags = "Status")
@Path("/")
public class KeyCoordinatorStatusToggleResource {

  private static final Logger LOGGER = LoggerFactory.getLogger(KeyCoordinatorStatusToggleResource.class);

  @Inject
  private KeyCoordinatorClusterHelixManager _keyCoordinatorClusterHelixManager;

  @POST
  @Path("/start")
  @Produces(MediaType.TEXT_PLAIN)
  @ApiOperation(value = "Start consuming key coordinator messages", produces = MediaType.TEXT_PLAIN,
      notes = "Start consuming key coordinator messages")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Success"),
      @ApiResponse(code = 500, message = "Internal error")
  })
  public String startConsuming() {
    LOGGER.info("Trying to start consuming key coordinator messages");
    _keyCoordinatorClusterHelixManager.rebalance();
    return "Successfully started consuming key coordinator messages";
  }
}
