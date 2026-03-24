"""
Excel workbook generator for financial statements.

Generates multi-sheet GAAP Excel workbooks with 4 sheets:
- Income Statement
- Balance Sheet  
- Financial Ratios
- 3-Year Comparison
"""
from __future__ import annotations

import csv
import random
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from company_generator import Company, FinancialHistory


@dataclass
class FinancialRatios:
    """Calculated financial ratios."""
    current_ratio: float
    quick_ratio: float
    debt_to_equity: float
    debt_to_assets: float
    return_on_assets: float
    return_on_equity: float
    gross_margin: float
    operating_margin: float
    net_margin: float
    asset_turnover: float
    interest_coverage: float


class ExcelGenerator:
    """Generate multi-sheet GAAP Excel workbooks (as CSV files)."""
    
    def __init__(self, random_seed: int = 42):
        self.rng = random.Random(random_seed)
        self.generated_files: list[Path] = []
    
    def _format_currency(self, amount: float) -> str:
        """Format amount as currency string."""
        return f"${amount:,.2f}"
    
    def _format_percent(self, value: float) -> str:
        """Format value as percentage."""
        return f"{value*100:.2f}%"
    
    def _calculate_ratios(
        self,
        company: Company,
        financials: FinancialHistory
    ) -> FinancialRatios:
        """Calculate financial ratios from company data."""
        
        # Use total_assets as proxy for current assets
        current_assets = financials.total_assets * self.rng.uniform(0.25, 0.40)
        current_liabilities = financials.total_liabilities * self.rng.uniform(0.20, 0.35)
        inventory = current_assets * self.rng.uniform(0.15, 0.30) if company.industry in ["Retail", "Manufacturing"] else 0
        
        total_equity = financials.total_assets - financials.total_liabilities
        interest_expense = financials.total_liabilities * self.rng.uniform(0.03, 0.06)
        
        # EBIT for interest coverage
        ebit = financials.ebitda * self.rng.uniform(0.70, 0.85)
        
        return FinancialRatios(
            current_ratio=current_assets / max(current_liabilities, 1),
            quick_ratio=(current_assets - inventory) / max(current_liabilities, 1),
            debt_to_equity=financials.total_liabilities / max(total_equity, 1),
            debt_to_assets=financials.total_liabilities / max(financials.total_assets, 1),
            return_on_assets=financials.net_income / max(financials.total_assets, 1),
            return_on_equity=financials.net_income / max(total_equity, 1),
            gross_margin=self.rng.uniform(0.35, 0.55),
            operating_margin=financials.ebitda / max(financials.revenue, 1),
            net_margin=financials.net_income / max(financials.revenue, 1),
            asset_turnover=financials.revenue / max(financials.total_assets, 1),
            interest_coverage=ebit / max(interest_expense, 1),
        )
    
    def _generate_income_sheet(
        self,
        company: Company,
        financials: FinancialHistory
    ) -> list[list[str]]:
        """Generate income statement sheet data."""
        revenue = financials.revenue
        cogs = revenue * self.rng.uniform(0.45, 0.65)
        gross_profit = revenue - cogs
        
        opex = revenue * self.rng.uniform(0.25, 0.45)
        operating_income = gross_profit - opex
        
        interest_expense = financials.total_liabilities * self.rng.uniform(0.03, 0.06)
        interest_income = financials.total_assets * self.rng.uniform(0.005, 0.015)
        
        pretax = operating_income - interest_expense + interest_income
        tax = pretax * 0.21
        net_income = pretax - tax
        
        return [
            ["INCOME STATEMENT", "", f"FY{financials.year}"],
            ["Company:", company.name, ""],
            ["", "", ""],
            ["Revenue", self._format_currency(revenue), "100.00%"],
            ["Cost of Goods Sold", self._format_currency(cogs), f"{(cogs/revenue)*100:.2f}%"],
            ["Gross Profit", self._format_currency(gross_profit), f"{(gross_profit/revenue)*100:.2f}%"],
            ["", "", ""],
            ["Operating Expenses", self._format_currency(opex), f"{(opex/revenue)*100:.2f}%"],
            ["Operating Income", self._format_currency(operating_income), f"{(operating_income/revenue)*100:.2f}%"],
            ["", "", ""],
            ["Interest Expense", self._format_currency(interest_expense), ""],
            ["Interest Income", self._format_currency(interest_income), ""],
            ["", "", ""],
            ["Income Before Taxes", self._format_currency(pretax), f"{(pretax/revenue)*100:.2f}%"],
            ["Income Tax Expense", self._format_currency(tax), f"{(tax/revenue)*100:.2f}%"],
            ["NET INCOME", self._format_currency(net_income), f"{(net_income/revenue)*100:.2f}%"],
        ]
    
    def _generate_balance_sheet(
        self,
        company: Company,
        financials: FinancialHistory
    ) -> list[list[str]]:
        """Generate balance sheet data."""
        cash = financials.operating_cash_flow * self.rng.uniform(0.8, 1.2)
        ar = financials.revenue * self.rng.uniform(0.08, 0.15)
        inventory = financials.revenue * self.rng.uniform(0.05, 0.12) if company.industry in ["Retail", "Manufacturing"] else 0
        current_assets = cash + ar + inventory + (financials.revenue * 0.02)
        
        ppe = financials.total_assets * self.rng.uniform(0.40, 0.60)
        intangibles = financials.total_assets * self.rng.uniform(0.05, 0.15)
        
        ap = financials.revenue * self.rng.uniform(0.05, 0.10)
        accrued = financials.revenue * self.rng.uniform(0.03, 0.06)
        std = financials.total_liabilities * self.rng.uniform(0.10, 0.25)
        current_liab = ap + accrued + std + (financials.total_liabilities * 0.08)
        
        ltd = financials.total_liabilities * self.rng.uniform(0.40, 0.60)
        
        equity = financials.total_assets - financials.total_liabilities
        
        rows = [
            ["BALANCE SHEET", "", f"As of Dec 31, {financials.year}"],
            ["Company:", company.name, ""],
            ["", "", ""],
            ["ASSETS", "", ""],
            ["Current Assets:", "", ""],
            ["  Cash", self._format_currency(cash), ""],
            ["  Accounts Receivable", self._format_currency(ar), ""],
        ]
        
        if inventory > 0:
            rows.append(["  Inventory", self._format_currency(inventory), ""])
        
        rows.extend([
            ["Total Current Assets", self._format_currency(current_assets), ""],
            ["", "", ""],
            ["Non-Current Assets:", "", ""],
            ["  PP&E", self._format_currency(ppe), ""],
            ["  Intangible Assets", self._format_currency(intangibles), ""],
            ["Total Non-Current Assets", self._format_currency(ppe + intangibles), ""],
            ["TOTAL ASSETS", self._format_currency(financials.total_assets), ""],
            ["", "", ""],
            ["LIABILITIES", "", ""],
            ["Current Liabilities:", "", ""],
            ["  Accounts Payable", self._format_currency(ap), ""],
            ["  Accrued Expenses", self._format_currency(accrued), ""],
            ["  Short-term Debt", self._format_currency(std), ""],
            ["Total Current Liabilities", self._format_currency(current_liab), ""],
            ["", "", ""],
            ["Non-Current Liabilities:", "", ""],
            ["  Long-term Debt", self._format_currency(ltd), ""],
            ["Total Non-Current Liabilities", self._format_currency(ltd), ""],
            ["TOTAL LIABILITIES", self._format_currency(financials.total_liabilities), ""],
            ["", "", ""],
            ["EQUITY", "", ""],
            ["Total Stockholders' Equity", self._format_currency(equity), ""],
            ["TOTAL LIABILITIES & EQUITY", self._format_currency(financials.total_liabilities + equity), ""],
        ])
        
        return rows
    
    def _generate_ratios_sheet(
        self,
        company: Company,
        financials: FinancialHistory
    ) -> list[list[str]]:
        """Generate financial ratios sheet."""
        ratios = self._calculate_ratios(company, financials)
        
        return [
            ["FINANCIAL RATIOS", "", f"FY{financials.year}"],
            ["Company:", company.name, ""],
            ["", "", ""],
            ["Liquidity Ratios", "", ""],
            ["Current Ratio", f"{ratios.current_ratio:.2f}", "x"],
            ["Quick Ratio", f"{ratios.quick_ratio:.2f}", "x"],
            ["", "", ""],
            ["Leverage Ratios", "", ""],
            ["Debt-to-Equity", f"{ratios.debt_to_equity:.2f}", "x"],
            ["Debt-to-Assets", self._format_percent(ratios.debt_to_assets), ""],
            ["Interest Coverage", f"{ratios.interest_coverage:.2f}", "x"],
            ["", "", ""],
            ["Profitability Ratios", "", ""],
            ["Return on Assets (ROA)", self._format_percent(ratios.return_on_assets), ""],
            ["Return on Equity (ROE)", self._format_percent(ratios.return_on_equity), ""],
            ["Gross Margin", self._format_percent(ratios.gross_margin), ""],
            ["Operating Margin", self._format_percent(ratios.operating_margin), ""],
            ["Net Margin", self._format_percent(ratios.net_margin), ""],
            ["", "", ""],
            ["Efficiency Ratios", "", ""],
            ["Asset Turnover", f"{ratios.asset_turnover:.2f}", "x"],
        ]
    
    def _generate_comparison_sheet(
        self,
        company: Company
    ) -> list[list[str]]:
        """Generate 3-year comparison sheet."""
        if len(company.financial_history) < 3:
            # Generate placeholder if not enough history
            years = [datetime.now().year - 2, datetime.now().year - 1, datetime.now().year]
        else:
            years = [fh.year for fh in company.financial_history]
        
        rows = [
            ["3-YEAR FINANCIAL COMPARISON", "", "", ""],
            ["Company:", company.name, "", ""],
            ["", "", "", ""],
            ["", str(years[0]), str(years[1]), str(years[2])],
        ]
        
        # Revenue row
        revenues = []
        for fh in company.financial_history[-3:]:
            revenues.append(self._format_currency(fh.revenue))
        while len(revenues) < 3:
            revenues.append("N/A")
        rows.append(["Revenue"] + revenues)
        
        # Net Income row
        incomes = []
        for fh in company.financial_history[-3:]:
            incomes.append(self._format_currency(fh.net_income))
        while len(incomes) < 3:
            incomes.append("N/A")
        rows.append(["Net Income"] + incomes)
        
        # Total Assets row
        assets = []
        for fh in company.financial_history[-3:]:
            assets.append(self._format_currency(fh.total_assets))
        while len(assets) < 3:
            assets.append("N/A")
        rows.append(["Total Assets"] + assets)
        
        # EBITDA row
        ebitdas = []
        for fh in company.financial_history[-3:]:
            ebitdas.append(self._format_currency(fh.ebitda))
        while len(ebitdas) < 3:
            ebitdas.append("N/A")
        rows.append(["EBITDA"] + ebitdas)
        
        # Growth rates
        rows.append(["", "", "", ""])
        rows.append(["Year-over-Year Growth", "", "", ""])
        
        growths = []
        for i, fh in enumerate(company.financial_history[-3:]):
            if i > 0:
                prev = company.financial_history[-3:][i-1].revenue
                growth = (fh.revenue - prev) / prev if prev > 0 else 0
                growths.append(self._format_percent(growth))
            else:
                growths.append("-")
        while len(growths) < 3:
            growths.append("N/A")
        rows.append(["Revenue Growth"] + growths)
        
        return rows
    
    def generate_excel_workbooks(
        self,
        companies: list[Company],
        output_dir: Path
    ) -> list[Path]:
        """Generate multi-sheet Excel workbooks (as CSV files)."""
        generated = []
        output_dir = Path(output_dir)
        
        for company in companies:
            if not company.financial_history:
                continue
            
            latest = company.financial_history[-1]
            
            # Create company directory
            company_dir = output_dir / "documents" / str(company.company_id)
            company_dir.mkdir(parents=True, exist_ok=True)
            
            # Generate each sheet
            sheets = {
                "Income": self._generate_income_sheet(company, latest),
                "Balance": self._generate_balance_sheet(company, latest),
                "Ratios": self._generate_ratios_sheet(company, latest),
                "Comparison": self._generate_comparison_sheet(company),
            }
            
            # Write combined workbook (as CSV with sheet markers)
            workbook_path = company_dir / "financial_statements.csv"
            with open(workbook_path, "w", newline="") as f:
                writer = csv.writer(f)
                for sheet_name, rows in sheets.items():
                    writer.writerow([f"--- SHEET: {sheet_name} ---"])
                    writer.writerows(rows)
                    writer.writerow([])
            
            generated.append(workbook_path)
            self.generated_files.append(workbook_path)
        
        return generated
    
    def generate_summary_csv(
        self,
        companies: list[Company],
        output_dir: Path
    ) -> list[Path]:
        """Generate financial summary CSV (most recent fiscal year)."""
        generated = []
        output_dir = Path(output_dir)
        
        for company in companies:
            if not company.financial_history:
                continue
            
            latest = company.financial_history[-1]
            
            # Create company directory
            company_dir = output_dir / "documents" / str(company.company_id)
            company_dir.mkdir(parents=True, exist_ok=True)
            
            # Generate summary
            summary_path = company_dir / "financial_summary.csv"
            with open(summary_path, "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(["Metric", "Value"])
                writer.writerow(["Company", company.name])
                writer.writerow(["Fiscal Year", latest.year])
                writer.writerow(["Revenue", latest.revenue])
                writer.writerow(["Net Income", latest.net_income])
                writer.writerow(["Total Assets", latest.total_assets])
                writer.writerow(["Total Liabilities", latest.total_liabilities])
                writer.writerow(["EBITDA", latest.ebitda])
                writer.writerow(["Operating Cash Flow", latest.operating_cash_flow])
                writer.writerow(["Profile Type", company.profile_type.value])
                writer.writerow(["Risk Level", company.risk_level.value])
            
            generated.append(summary_path)
            self.generated_files.append(summary_path)
        
        return generated
    
    def generate_all_excel(
        self,
        companies: list[Company],
        output_dir: Path
    ) -> dict[str, list[Path]]:
        """Generate all Excel/CSV documents."""
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        return {
            "workbooks": self.generate_excel_workbooks(companies, output_dir),
            "summaries": self.generate_summary_csv(companies, output_dir),
        }


if __name__ == "__main__":
    from company_generator import generate_companies
    
    companies = generate_companies(80, random_seed=42)
    
    generator = ExcelGenerator(random_seed=42)
    output_dir = Path("./test_output")
    
    results = generator.generate_all_excel(companies, output_dir)
    
    print("Generated Excel files:")
    for category, files in results.items():
        print(f"  {category}: {len(files)} files")
