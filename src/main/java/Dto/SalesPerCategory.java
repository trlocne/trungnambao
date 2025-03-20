package Dto;

import java.sql.Date;

public class SalesPerCategory {
    private Date date;
    private String category;
    private double totalSales;

    public SalesPerCategory() {
    }

    public SalesPerCategory(Date date, String category, double totalSales) {
        this.date = date;
        this.category = category;
        this.totalSales = totalSales;
    }

    public Date getDate() {
        return date;
    }

    public void setDate(Date date) {
        this.date = date;
    }

    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    public double getTotalSales() {
        return totalSales;
    }

    public void setTotalSales(double totalSales) {
        this.totalSales = totalSales;
    }
}
