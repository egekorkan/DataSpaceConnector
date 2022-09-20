/*
 *  Copyright (c) 2021 Fraunhofer Institute for Software and Systems Engineering
 *
 *  This program and the accompanying materials are made available under the
 *  terms of the Apache License, Version 2.0 which is available at
 *  https://www.apache.org/licenses/LICENSE-2.0
 *
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Contributors:
 *       Fraunhofer Institute for Software and Systems Engineering - initial API and implementation
 *
 */

package org.eclipse.dataspaceconnector.extensions.api;

import org.eclipse.dataspaceconnector.dataplane.spi.pipeline.DataTransferExecutorServiceContainer;
import org.eclipse.dataspaceconnector.dataplane.spi.pipeline.PipelineService;
import org.eclipse.dataspaceconnector.policy.model.Action;
import org.eclipse.dataspaceconnector.policy.model.AtomicConstraint;
import org.eclipse.dataspaceconnector.policy.model.LiteralExpression;
import org.eclipse.dataspaceconnector.policy.model.Operator;
import org.eclipse.dataspaceconnector.policy.model.Permission;
import org.eclipse.dataspaceconnector.policy.model.Policy;
import org.eclipse.dataspaceconnector.spi.asset.AssetIndex;
import org.eclipse.dataspaceconnector.spi.asset.AssetSelectorExpression;
import org.eclipse.dataspaceconnector.spi.contract.offer.store.ContractDefinitionStore;
import org.eclipse.dataspaceconnector.spi.policy.PolicyContext;
import org.eclipse.dataspaceconnector.spi.policy.PolicyDefinition;
import org.eclipse.dataspaceconnector.spi.policy.PolicyEngine;
import org.eclipse.dataspaceconnector.spi.policy.store.PolicyDefinitionStore;
import org.eclipse.dataspaceconnector.spi.system.Inject;
import org.eclipse.dataspaceconnector.spi.system.ServiceExtension;
import org.eclipse.dataspaceconnector.spi.system.ServiceExtensionContext;
import org.eclipse.dataspaceconnector.spi.types.domain.DataAddress;
import org.eclipse.dataspaceconnector.spi.types.domain.asset.Asset;
import org.eclipse.dataspaceconnector.spi.types.domain.contract.offer.ContractDefinition;
import static org.eclipse.dataspaceconnector.spi.policy.PolicyEngine.ALL_SCOPES;

import java.nio.file.Path;
import java.util.Objects;

public class FileTransferExtension implements ServiceExtension {

    public static final String USE_POLICY = "use-eu";
    private static final String EDC_ASSET_PATH = "edc.samples.04.asset.path";
    @Inject
    private ContractDefinitionStore contractStore;
    @Inject
    private AssetIndex assetIndex;
    @Inject
    private PipelineService pipelineService;
    @Inject
    private DataTransferExecutorServiceContainer executorContainer;
    @Inject
    private PolicyDefinitionStore policyStore;

    @Override
    public void initialize(ServiceExtensionContext context) {
        var monitor = context.getMonitor();

        var sourceFactory = new FileTransferDataSourceFactory();
        pipelineService.registerFactory(sourceFactory);

        var sinkFactory = new FileTransferDataSinkFactory(monitor, executorContainer.getExecutorService(), 5);
        pipelineService.registerFactory(sinkFactory);

        // the next 4 lines can be commented out to have a connector start without any policy etc.
        var policy = createPolicy();
        policyStore.save(policy);

        registerDataEntries(context);
        registerContractDefinition(policy.getUid());


        context.getMonitor().info("File Transfer Extension initialized!");

        var policyEngine = context.getService(PolicyEngine.class);
        policyEngine.registerFunction(
                ALL_SCOPES,
                Permission.class,
                "region",
                (Operator operator, Object rightOperand, Permission permission, PolicyContext context2) -> {
                        var consumerRegion = context2.getParticipantAgent().getClaims().get("region"); // #1
                        switch (operator) {
                                case EQ:
                                        return Objects.equals(consumerRegion, rightOperand);
                                default:
                                        return false;
                        }
                });
    }

    private PolicyDefinition createPolicy() {

        // var movePermission = Permission.Builder.newInstance()
        //         .action(Action.Builder.newInstance().type("MOVE").build())
        //         .build();

        // var copyPermission = Permission.Builder.newInstance()
        //         .action(Action.Builder.newInstance().type("COPY").build())
        //         .build();
        
        var euConstraint = AtomicConstraint.Builder.newInstance()
                .leftExpression(new LiteralExpression("region"))
                .operator(Operator.EQ)
                .rightExpression(new LiteralExpression("eu"))
                .build();

        var usePermission = Permission.Builder.newInstance()
                .action(Action.Builder.newInstance().type("USE").build())
                .constraint(euConstraint)
                .build();

        // add a region constraint here, add eu as a mock region in consumer properties.

        return PolicyDefinition.Builder.newInstance()
                .id(USE_POLICY)
                .policy(Policy.Builder.newInstance()
                        .permission(usePermission)
                        .build())
                .build();
    }

    private void registerDataEntries(ServiceExtensionContext context) {
        var assetPathSetting = context.getSetting(EDC_ASSET_PATH, "/tmp/provider/test-document.txt");
        var assetPath = Path.of(assetPathSetting);

        var dataAddress = DataAddress.Builder.newInstance()
                .property("type", "File")
                .property("path", assetPath.getParent().toString())
                .property("filename", assetPath.getFileName().toString())
                .build();

        var assetId = "test-document";
        var asset = Asset.Builder.newInstance().id(assetId).build();

        assetIndex.accept(asset, dataAddress);
    }

    private void registerContractDefinition(String uid) {
        var contractDefinition = ContractDefinition.Builder.newInstance()
                .id("1")
                .accessPolicyId(uid)
                .contractPolicyId(uid)
                .selectorExpression(AssetSelectorExpression.Builder.newInstance()
                        .whenEquals(Asset.PROPERTY_ID, "test-document") 
                        // selector can be used for a set of data with a certain file name
                        .build())
                .build();

        contractStore.save(contractDefinition);
    }
}
