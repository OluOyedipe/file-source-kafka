/*
 * Copyright 2015-2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.cloud.stream.app.file.source.kafka;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.app.file.FileConsumerProperties;
import org.springframework.cloud.stream.app.file.FileReadingMode;
import org.springframework.cloud.stream.app.file.FileUtils;
import org.springframework.cloud.stream.app.file.source.FileSourceProperties;
import org.springframework.cloud.stream.app.trigger.TriggerConfiguration;
import org.springframework.cloud.stream.app.trigger.TriggerPropertiesMaxMessagesDefaultUnlimited;
import org.springframework.cloud.stream.messaging.Source;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlowBuilder;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.file.DirectoryScanner;
import org.springframework.integration.file.FileReadingMessageSource;
import org.springframework.integration.file.RecursiveDirectoryScanner;
import org.springframework.integration.file.dsl.FileInboundChannelAdapterSpec;
import org.springframework.integration.file.dsl.Files;
import org.springframework.integration.file.filters.*;
import org.springframework.integration.mongodb.metadata.MongoDbMetadataStore;
import org.springframework.util.StringUtils;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * Creates a {@link FileReadingMessageSource} bean and registers it as a
 * Inbound Channel Adapter that sends messages to the Source output channel.
 *
 * @author Gary Russell
 * @author Artem Bilan
 */
@EnableBinding(Source.class)
@Import(TriggerConfiguration.class)
@EnableConfigurationProperties({ FileSourceProperties.class, FileConsumerProperties.class,
        TriggerPropertiesMaxMessagesDefaultUnlimited.class })
public class MyFileSourceConfiguration {

    @Autowired
    private FileSourceProperties properties;

    @Autowired
    private FileConsumerProperties fileConsumerProperties;

    @Autowired
    Source source;

    @Autowired
    MongoDbMetadataStore metadataStore;

    @Bean
    public IntegrationFlow fileSourceFlow() {
        FileInboundChannelAdapterSpec messageSourceSpec = Files.inboundAdapter(new File(this.properties.getDirectory()));

        messageSourceSpec.scanner(directoryScanner());

        IntegrationFlowBuilder flowBuilder = IntegrationFlows.from(messageSourceSpec);

        if (this.fileConsumerProperties.getMode() != FileReadingMode.ref) {
            flowBuilder = FileUtils.enhanceFlowForReadingMode(flowBuilder, this.fileConsumerProperties);
        }

        return flowBuilder.channel(source.output()).get();
    }


    @Bean
    CompositeFileListFilter<File> compositeFileListFilter() {
        final List<FileListFilter<File>> defaultFilters = new ArrayList<>(2);
        defaultFilters.add(new IgnoreHiddenFileListFilter());
//        defaultFilters.add(new AcceptOnceFileListFilter<>());
        defaultFilters.add(new FileSystemPersistentAcceptOnceFileListFilter(metadataStore, "seen-files"));
        if (StringUtils.hasText(this.properties.getFilenamePattern())) {
            defaultFilters.add(new SimplePatternFileListFilter(this.properties.getFilenamePattern()));
        }

        return new CompositeFileListFilter<>(defaultFilters);
    }


    @Bean
    DirectoryScanner directoryScanner() {
        DirectoryScanner scanner = new RecursiveDirectoryScanner();
        scanner.setFilter(compositeFileListFilter());

        return scanner;
    }

    @Bean
    public MongoDbMetadataStore metadataStore(MongoDbFactory factory) {
        return new MongoDbMetadataStore(factory, "integrationMetadataStore");
    }

}
