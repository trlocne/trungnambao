package Dto;

import java.sql.Date;

public class SalesPerDay {
    private Date transactionDate;
    private double totalSales;

    public SalesPerDay() {
    }

    public SalesPerDay(Date transactionDate, double totalSales) {
        this.transactionDate = transactionDate;
        this.totalSales = totalSales;
    }

    public Date getTransactionDate() {
        return transactionDate;
    }

    public void setTransactionDate(Date transactionDate) {
        this.transactionDate = transactionDate;
    }

    public double getTotalSales() {
        return totalSales;
    }

    public void setTotalSales(double totalSales) {
        this.totalSales = totalSales;
    }
}
