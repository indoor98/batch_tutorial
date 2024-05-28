package com.tutorial.batchflow.domain;


import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Pattern;
import lombok.*;

@Getter
@Setter
@ToString
@AllArgsConstructor
@NoArgsConstructor
public class Product {

    private Integer productId;
    private String productName;
    @Pattern(regexp = "Mobile Phones|Tablets|Televisions|Sports Accessorry")
    private String productCategory;
    @Max(100000)
    private Integer productPrice;
}
