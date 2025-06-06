package com.dev.pranay.Multithreaded.Batched.Processing.services;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import com.dev.pranay.Multithreaded.Batched.Processing.entities.Product;
import com.dev.pranay.Multithreaded.Batched.Processing.entities.ProductDocument;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

@Service
@Slf4j
@RequiredArgsConstructor
public class ProductBulkIndexService { //Native Bulk Service

    private final ElasticsearchClient elasticsearchClient;

    public void indexBatch(List<ProductDocument> products) throws IOException {
        if(products.isEmpty()) return;

        List<BulkOperation> bulkOperations = products.stream()
                .map(p -> BulkOperation.of(op -> op
                        .index(idx -> idx
                                .index("products")
                                .id(String.valueOf(p.getId()))
                                .document(p)
                        )))
                .collect(Collectors.toList());

        BulkResponse bulkResponse = elasticsearchClient.bulk(b -> b.operations(bulkOperations));

        if(bulkResponse.errors()) {
            log.error("Bulk indexing had errors.");
            bulkResponse.items().forEach(item -> {
                if(item.error() != null) {
                    log.error("Failed to index ID {}: %s {}, Reason: %s%n", item.id(), item.error().reason());
                }
            });
        } else {
            log.error(" Indexed {} documents", products.size());
        }
    }
}
