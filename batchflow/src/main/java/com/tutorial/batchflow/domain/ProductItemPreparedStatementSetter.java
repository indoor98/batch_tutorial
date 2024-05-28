package com.tutorial.batchflow.domain;

import org.springframework.batch.item.database.ItemPreparedStatementSetter;
import org.springframework.jdbc.core.PreparedStatementSetter;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class ProductItemPreparedStatementSetter implements ItemPreparedStatementSetter<Product> {

    @Override
    public void setValues(Product item, PreparedStatement ps) throws SQLException {
        ps.setInt(1, item.getProductId());
        ps.setString(2, item.getProductName());
        ps.setString(3, item.getProductCategory());
        ps.setInt(4, item.getProductPrice());
    }
}
