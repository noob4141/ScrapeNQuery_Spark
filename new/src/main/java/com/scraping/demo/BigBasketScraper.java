package com.scraping.demo;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

public class BigBasketScraper {

    public static void main(String[] args) {
        // Scrape data and save to CSV at regular intervals
        Timer timer = new Timer();
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                scrapeAndSaveData();
            }
        }, 0, 60000); // Run every 60 seconds (60000 milliseconds)
    }

    private static void scrapeAndSaveData() {
        // List of URLs to scrape
        List<String> urls = Arrays.asList(
            "https://www.bigbasket.com/cl/bakery-cakes-dairy/?nc=nb",
            "https://www.bigbasket.com/cl/snacks-branded-foods/?nc=nb",
            "https://www.bigbasket.com/cl/beverages/?nc=nb",
            "https://www.bigbasket.com/cl/foodgrains-oil-masala/?nc=nb",
            "https://www.bigbasket.com/cl/eggs-meat-fish/?nc=nb",
            "https://www.bigbasket.com/cl/fruits-vegetables/?nc=nb"
        );

        // Create a CSV file to save the extracted data
        try (BufferedWriter writer = Files.newBufferedWriter(Paths.get("D:\\bigbasket\\new21.csv"), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
             CSVPrinter csvPrinter = new CSVPrinter(writer, CSVFormat.DEFAULT.withHeader("Index", "Product", "Brand", "Sale_Price", "Market_Price", "Discount (%)", "Quantity", "No_of_Ratings", "Product_Ratings"))) {

            int index = 1; // Initialize index counter

            for (String url : urls) {
                try {
                    // Fetch the HTML code from the URL using JSoup
                    Document doc = Jsoup.connect(url)
                            .userAgent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
                            .get();

                    Elements products = doc.select("li.PaginateItems___StyledLi-sc-1yrbjdr-0");

                    for (Element product : products) {
                        // Extract product details
                        String brand = product.select("span.BrandName___StyledLabel2-sc-hssfrl-1").text(); 
                        String productName = product.select("h3.block.m-0.line-clamp-2").text(); 
                        String priceText = product.select("span.Pricing___StyledLabel-sc-pldi2d-1").text(); 
                        String marketPriceText = product.select("span.Pricing___StyledLabel2-sc-pldi2d-2").text(); 
                        String discountText = product.select("span.Tags___StyledLabel2-sc-aeruf4-1").text(); 
                        String quantity = product.select("span.PackChanger___StyledLabel-sc-newjpv-1").text(); 
                        String noOfRatingsText = product.select("span.ReviewsAndRatings___StyledLabel-sc-2rprpc-1").text(); 
                        String productRatings = product.select("span.Badges___StyledLabel-sc-1k3p1ug-0").text(); 

                        
                        String salePrice = priceText.replaceAll("[^\\d.]", ""); 
                        String marketPrice = marketPriceText.replaceAll("[^\\d.]", ""); 
                        String discount = discountText.replaceAll("[^\\d]", ""); 
                        String noOfRatings = noOfRatingsText.replaceAll("[^\\d]", ""); 

                        // Print details to CSV
                        csvPrinter.printRecord(index++, productName, brand, salePrice, marketPrice, discount, quantity, noOfRatings, productRatings);
                    }

                } catch (IOException e) {
                    System.err.println("Error during data scraping from URL: " + url + " - " + e.getMessage());
                    e.printStackTrace();
                }
            }
            System.out.println("Data scraped and saved to CSV.");

        } catch (IOException e) {
            System.err.println("Error during CSV writing: " + e.getMessage());
            e.printStackTrace();
        }
    }
}