package com.dev.pranay.Multithreaded.Batched.Processing.controller;

import com.dev.pranay.Multithreaded.Batched.Processing.entities.ProductDocument;
import com.dev.pranay.Multithreaded.Batched.Processing.services.ProductIndexService;
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
}
