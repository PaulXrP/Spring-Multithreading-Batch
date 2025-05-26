package com.dev.pranay.Multithreaded.Batched.Processing.controller;

import com.dev.pranay.Multithreaded.Batched.Processing.services.ProductService;
import com.dev.pranay.Multithreaded.Batched.Processing.services.ProductServiceV2;
import com.dev.pranay.Multithreaded.Batched.Processing.services.ProductServiceV3;
import com.dev.pranay.Multithreaded.Batched.Processing.services.ProductionCsvProcessingService;
import lombok.RequiredArgsConstructor;
import org.springframework.data.repository.query.Param;
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

    private final ProductServiceV3 productServiceV3;

    private final ProductionCsvProcessingService processingService;

    @PostMapping("/save-csv-batch")
    public ResponseEntity<String> saveProductsFromCsvToDbInBatch(@RequestParam("filepath")
                                                                 String filePath) {
        String saved = productService.saveProductFromCsvInBatch(filePath);
        return new ResponseEntity<>(saved, HttpStatus.OK);
    }

    @PostMapping("/save-csv-batch2")
    public ResponseEntity<String> saveProductsFromCsvToDbInBatch2(@RequestParam("filepath")
                                                                 String filePath) {
        String saved = productServiceV3.saveProductFromCsvInBatch(filePath);
        return new ResponseEntity<>(saved, HttpStatus.OK);
    }

    @PostMapping("/save-csv-batch-multithreading-using-ExecutorService")
    public ResponseEntity<String> saveProductsFromCsvToDbInBatch3(@RequestParam("filepath")
                                                                  String filePath) {
        String saved = productServiceV3.saveProductFromCsvInBatchMultithreaded(filePath);
        return new ResponseEntity<>(saved, HttpStatus.OK);
    }

    @PostMapping("/save-csv-batch-streaming-plus-multithreading-using-ExecutorService")
    public ResponseEntity<String> saveProductsFromCsvToDbInBatch4(@RequestParam("filepath")
                                                                  String filePath) {
        String saved = productServiceV3.loadCsvStreamingInChunks(filePath);
        return new ResponseEntity<>(saved, HttpStatus.OK);
    }

    @PostMapping("/save-csv-batch-streaming-using-ExecutorService-production-grade")
    public ResponseEntity<String> saveProductsFromCsvToDbInBatch5(@RequestParam("filepath")
                                                                  String filePath) {
        String saved = processingService.loadCsvStreamingInChunksProduction(filePath);
        return new ResponseEntity<>(saved, HttpStatus.OK);
    }

    @PostMapping("/save-csv-batch-via-Jdbc-template")
    public ResponseEntity<String> saveProductsFromCsvToDbInBatchJdbc(@RequestParam("filepath")
                                                                 String filePath) {
        String saved = productService.saveProductFromCsvInBatchViaJdbcTemplate(filePath);
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


    @GetMapping("/resetJPQL")
    public ResponseEntity<String> dataResetJPQL() {
        String resetRecords = productService.resetRecordsJPQL();
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

    @DeleteMapping("/deleteInBulk")
    public ResponseEntity<String> clearDbOptimise() {
        String deleted = productService.deleteDbOptimized();
        return new ResponseEntity<>(deleted, HttpStatus.OK);
    }


    @DeleteMapping("/deleteInBatch")
    public ResponseEntity<String> clearDbInBatch() {
        String deleted = productService.deleteByBatch();
        return new ResponseEntity<>(deleted, HttpStatus.OK);
    }

    @DeleteMapping("/deleteByPriceCondition")
    public ResponseEntity<String> deleteProductsCheaperThan(@RequestParam("price") @PathVariable
                                                                Double priceThreshold) {
        String deleted = productService.deleteProductsCheaperThan(priceThreshold);
        return new ResponseEntity<>(deleted, HttpStatus.OK);
    }

    @DeleteMapping("/deleteByCategory")
    public ResponseEntity<String> deleteProductsByCategory(@RequestParam("category")
                                                               @PathVariable String category) {
        String deleted = productService.deleteProductsByCategory(category);
        return new ResponseEntity<>(deleted, HttpStatus.OK);
    }

}
