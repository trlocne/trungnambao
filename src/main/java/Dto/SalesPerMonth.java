package Dto;

public class SalesPerMonth {
    private int year;
    private int month;
    private double totalSales;

    public SalesPerMonth() {
    }

    public SalesPerMonth(int year, int month, double totalSales) {
        this.year = year;
        this.month = month;
        this.totalSales = totalSales;
    }

    public int getYear() {
        return year;
    }

    public  void setYear(int year) {
        this.year = year;
    }

    public int getMonth() {
        return month;
    }

    public void setMonth(int month) {
        this.month = month;
    }

    public double getTotalSales() {
        return totalSales;
    }

    public void setTotalSales(double totalSales) {
        this.totalSales = totalSales;
    }

}
