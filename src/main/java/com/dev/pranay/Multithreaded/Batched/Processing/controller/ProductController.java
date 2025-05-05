package com.dev.pranay.Multithreaded.Batched.Processing.controller;

import com.dev.pranay.Multithreaded.Batched.Processing.services.ProductService;
import com.dev.pranay.Multithreaded.Batched.Processing.services.ProductServiceV2;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/products")
@RequiredArgsConstructor
public class ProductController {

    private final ProductService productService;

    private final ProductServiceV2 productServiceV2;

    @PostMapping("/save-csv-batch")
    public ResponseEntity<String> saveProductsFromCsvToDbInBatch(@RequestParam("filepath")
                                                                 String filePath) {
        String saved = productService.saveProductFromCsvInBatch(filePath);
        return new ResponseEntity<>(saved, HttpStatus.OK);
    }

    @GetMapping("/allIds")
    public ResponseEntity<List<Long>> getAllProductIds() {
        List<Long> allIds = productService.getAllIds();
        return new ResponseEntity<>(allIds, HttpStatus.FOUND);
    }

    @GetMapping("/reset")
    public ResponseEntity<String> dataReset() {
        String resetRecords = productService.resetRecords();
        return new ResponseEntity<>(resetRecords, HttpStatus.OK);
    }

    @PostMapping("/process")   //705 ms for 1000 records
    public ResponseEntity<String> processIds(@RequestBody List<Long> productIds) {
        String processed = productService.processProductIds(productIds);
        return new ResponseEntity<>(processed, HttpStatus.OK);
    }

    @PostMapping("/process/v2") //431 ms for 1000 records
    public ResponseEntity<String> processIdsV2(@RequestBody List<Long> productIds) {
        String processed = productServiceV2.executeProductIds(productIds);
        return new ResponseEntity<>(processed, HttpStatus.OK);
    }

    @DeleteMapping("/delete")
    public ResponseEntity<String> clearDb() {
        String deleted = productService.deleteDb();
        return new ResponseEntity<>(deleted, HttpStatus.OK);
    }

}
