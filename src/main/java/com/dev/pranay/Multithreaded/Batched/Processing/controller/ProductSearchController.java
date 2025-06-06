package com.dev.pranay.Multithreaded.Batched.Processing.controller;

import com.dev.pranay.Multithreaded.Batched.Processing.entities.ProductDocument;
import com.dev.pranay.Multithreaded.Batched.Processing.services.ProductIndexService;
import com.dev.pranay.Multithreaded.Batched.Processing.services.ProductIndexService2;
import com.dev.pranay.Multithreaded.Batched.Processing.services.ProductIndexService3;
import com.dev.pranay.Multithreaded.Batched.Processing.services.ProductSearchService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/products/search")
@RequiredArgsConstructor
public class ProductSearchController {

    private final ProductSearchService productSearchService;

    private final ProductIndexService productIndexService;

    private final ProductIndexService2 productIndexService2;

    private final ProductIndexService3 productIndexService3;

    @GetMapping
    public ResponseEntity<List<ProductDocument>> search(
            @RequestParam(required = false) String name,
            @RequestParam(required = false) String category,
            @RequestParam(required = false) Double minPrice,
            @RequestParam(required = false) Double maxPrice,
            @RequestParam(required = false) Boolean offerApplied
    ) {
        List<ProductDocument> productDocuments = productSearchService.searchProducts(name, category, minPrice, maxPrice, offerApplied);
        return new ResponseEntity<>(productDocuments, HttpStatus.FOUND);
    }

    @PostMapping("/reindex") //Example endpoint to trigger re-indexing
    public ResponseEntity<String> reindexAll() {
        productIndexService.indexAllProducts();
        return ResponseEntity.ok("Re-indexing initiated.");
    }

    @PostMapping("/reindexMultithreaded") //Example endpoint to trigger re-indexing
    public ResponseEntity<String> reindexAllMultithreaded() {
        productIndexService.indexAllProductsMultithreaded();
        return ResponseEntity.ok("Re-indexing via multi-threaded initiated.");
    }

    @PostMapping("/reindexMultithreaded2") //Example endpoint to trigger re-indexing
    public ResponseEntity<String> reindexAllMultithreaded2() {
        String indexed = productIndexService2.indexAllProductsConcurrently();
        return new ResponseEntity<>(indexed, HttpStatus.OK);
    }

    @PostMapping("/reindexMultithreadedNativeBulk") //Example endpoint to trigger re-indexing
    public ResponseEntity<String> reindexAllMultithreadedNativeBulk() {
        String indexed = productIndexService2.indexAllProducts();
        return new ResponseEntity<>(indexed, HttpStatus.OK);
    }

    @PostMapping("/reindexAllMultithreadedNativeBulk2") //Example endpoint to trigger re-indexing
    public ResponseEntity<String> reindexAllMultithreadedNativeBulk2() {
        String indexed = productIndexService3.indexAllProductsConcurrently();
        return new ResponseEntity<>(indexed, HttpStatus.OK);
    }
}
