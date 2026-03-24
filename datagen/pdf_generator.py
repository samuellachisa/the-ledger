"""
PDF document generator for financial statements.

Generates GAAP-compliant PDFs with variants:
- Income Statement (4 variants: clean:40, dense:20, missing_ebitda:8, scanned:12)
- Balance Sheet (6 with equity rounding discrepancy $500-$4,500)
- Application Proposal PDF (one per seeded application)
"""
from __future__ import annotations

import random
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any
from uuid import UUID

from company_generator import Company, FinancialHistory


@dataclass
class PDFVariant:
    name: str
    count: int
    description: str


# Income statement variants
INCOME_STATEMENT_VARIANTS = [
    PDFVariant("clean", 40, "Standard GAAP income statement"),
    PDFVariant("dense", 20, "Dense layout with minimal whitespace"),
    PDFVariant("missing_ebitda", 8, "Income statement without EBITDA line"),
    PDFVariant("scanned", 12, "Simulated scanned document appearance"),
]


class PDFGenerator:
    """Generate GAAP-compliant financial PDFs."""
    
    def __init__(self, random_seed: int = 42):
        self.rng = random.Random(random_seed)
        self.generated_files: list[Path] = []
    
    def _format_currency(self, amount: float) -> str:
        """Format amount as currency string."""
        if amount >= 1_000_000:
            return f"${amount/1_000_000:,.2f}M"
        elif amount >= 1_000:
            return f"${amount/1_000:,.1f}K"
        else:
            return f"${amount:,.2f}"
    
    def _generate_income_statement_content(
        self,
        company: Company,
        financials: FinancialHistory,
        variant: str
    ) -> str:
        """Generate income statement content in a structured format."""
        
        # Calculate line items
        revenue = financials.revenue
        cogs = revenue * self.rng.uniform(0.45, 0.65)
        gross_profit = revenue - cogs
        
        # Operating expenses
        salaries = revenue * self.rng.uniform(0.12, 0.20)
        marketing = revenue * self.rng.uniform(0.05, 0.12)
        rd = revenue * self.rng.uniform(0.03, 0.08) if company.industry == "Technology" else 0
        admin = revenue * self.rng.uniform(0.06, 0.10)
        depreciation = revenue * self.rng.uniform(0.03, 0.06)
        amortization = revenue * self.rng.uniform(0.01, 0.03)
        
        total_opex = salaries + marketing + rd + admin + depreciation + amortization
        operating_income = gross_profit - total_opex
        
        # Non-operating
        interest_expense = financials.total_liabilities * self.rng.uniform(0.03, 0.06)
        interest_income = financials.total_assets * self.rng.uniform(0.005, 0.015)
        
        # Taxes
        pretax_income = operating_income - interest_expense + interest_income
        tax_rate = 0.21 if self.rng.random() > 0.1 else self.rng.uniform(0.15, 0.28)
        tax_expense = max(0, pretax_income * tax_rate)
        
        net_income = pretax_income - tax_expense
        
        # Build content based on variant
        lines = []
        
        if variant == "scanned":
            lines.append("=" * 70)
            lines.append("  INCOME STATEMENT")
            lines.append("=" * 70)
            lines.append(f"  Company: {company.name}")
            lines.append(f"  Fiscal Year: {financials.year}")
            lines.append(f"  Generated: {datetime.now().strftime('%Y-%m-%d')}")
            lines.append("-" * 70)
        elif variant == "dense":
            lines.append("=" * 70)
            lines.append(f"INCOME STATEMENT - {company.name} - FY{financials.year}")
            lines.append("=" * 70)
        else:
            lines.append("=" * 70)
            lines.append("                     CONSOLIDATED INCOME STATEMENT")
            lines.append("=" * 70)
            lines.append(f"Company: {company.name}")
            lines.append(f"Fiscal Year Ended: December 31, {financials.year}")
            lines.append(f"Industry: {company.industry}")
            lines.append("-" * 70)
        
        lines.append("")
        lines.append(f"Revenue                                    {self._format_currency(revenue):>20}")
        lines.append(f"Cost of Goods Sold                         {self._format_currency(cogs):>20}")
        lines.append("-" * 70)
        lines.append(f"Gross Profit                               {self._format_currency(gross_profit):>20}")
        lines.append("")
        
        if variant != "dense":
            lines.append("Operating Expenses:")
        lines.append(f"  Salaries and Benefits                    {self._format_currency(salaries):>20}")
        lines.append(f"  Marketing and Sales                      {self._format_currency(marketing):>20}")
        if rd > 0:
            lines.append(f"  Research and Development                 {self._format_currency(rd):>20}")
        lines.append(f"  General and Administrative               {self._format_currency(admin):>20}")
        lines.append(f"  Depreciation                             {self._format_currency(depreciation):>20}")
        lines.append(f"  Amortization                             {self._format_currency(amortization):>20}")
        lines.append("-" * 70)
        lines.append(f"Total Operating Expenses                   {self._format_currency(total_opex):>20}")
        lines.append("")
        lines.append(f"Operating Income                           {self._format_currency(operating_income):>20}")
        lines.append("")
        
        if variant != "missing_ebitda":
            ebitda = operating_income + depreciation + amortization
            lines.append(f"EBITDA                                     {self._format_currency(ebitda):>20}")
            lines.append("")
        
        lines.append("Non-Operating Items:")
        lines.append(f"  Interest Expense                         {self._format_currency(interest_expense):>20}")
        lines.append(f"  Interest Income                          {self._format_currency(interest_income):>20}")
        lines.append("-" * 70)
        lines.append(f"Income Before Taxes                        {self._format_currency(pretax_income):>20}")
        lines.append(f"Income Tax Expense ({tax_rate*100:.1f}%)                {self._format_currency(tax_expense):>20}")
        lines.append("=" * 70)
        lines.append(f"NET INCOME                                 {self._format_currency(net_income):>20}")
        lines.append("=" * 70)
        
        if variant != "dense":
            lines.append("")
            lines.append("Notes:")
            lines.append("1. All amounts in USD")
            lines.append("2. Prepared in accordance with GAAP")
            lines.append(f"3. Tax rate: {tax_rate*100:.1f}%")
        
        return "\n".join(lines)
    
    def _generate_balance_sheet_content(
        self,
        company: Company,
        financials: FinancialHistory,
        add_discrepancy: bool = False
    ) -> str:
        """Generate balance sheet content with optional equity discrepancy."""
        
        # Assets
        cash = financials.operating_cash_flow * self.rng.uniform(0.8, 1.2)
        accounts_receivable = financials.revenue * self.rng.uniform(0.08, 0.15)
        inventory = financials.revenue * self.rng.uniform(0.05, 0.12) if company.industry in ["Retail", "Manufacturing"] else 0
        prepaid_expenses = financials.revenue * self.rng.uniform(0.01, 0.03)
        
        current_assets = cash + accounts_receivable + inventory + prepaid_expenses
        
        ppe = financials.total_assets * self.rng.uniform(0.40, 0.60)
        intangible_assets = financials.total_assets * self.rng.uniform(0.05, 0.15)
        investments = financials.total_assets * self.rng.uniform(0.05, 0.15)
        other_assets = financials.total_assets - current_assets - ppe - intangible_assets - investments
        
        total_assets = current_assets + ppe + intangible_assets + investments + other_assets
        
        # Liabilities
        accounts_payable = financials.revenue * self.rng.uniform(0.05, 0.10)
        accrued_expenses = financials.revenue * self.rng.uniform(0.03, 0.06)
        short_term_debt = financials.total_liabilities * self.rng.uniform(0.10, 0.25)
        current_portion_lt_debt = financials.total_liabilities * self.rng.uniform(0.05, 0.10)
        
        current_liabilities = accounts_payable + accrued_expenses + short_term_debt + current_portion_lt_debt
        
        long_term_debt = financials.total_liabilities * self.rng.uniform(0.40, 0.60)
        deferred_tax = financials.total_liabilities * self.rng.uniform(0.05, 0.10)
        other_liabilities = financials.total_liabilities - current_liabilities - long_term_debt - deferred_tax
        
        total_liabilities = current_liabilities + long_term_debt + deferred_tax + other_liabilities
        
        # Equity (with optional discrepancy)
        total_equity_calculated = total_assets - total_liabilities
        discrepancy = 0
        if add_discrepancy:
            discrepancy = self.rng.randint(500, 4500)
            if self.rng.random() > 0.5:
                discrepancy = -discrepancy
        
        total_equity = total_equity_calculated + discrepancy
        
        # Break down equity
        common_stock = total_equity * self.rng.uniform(0.05, 0.15)
        additional_paid_in = total_equity * self.rng.uniform(0.10, 0.25)
        retained_earnings = total_equity - common_stock - additional_paid_in
        
        lines = []
        lines.append("=" * 70)
        lines.append("                     CONSOLIDATED BALANCE SHEET")
        lines.append("=" * 70)
        lines.append(f"Company: {company.name}")
        lines.append(f"As of: December 31, {financials.year}")
        lines.append("-" * 70)
        lines.append("")
        
        lines.append("ASSETS")
        lines.append("-" * 70)
        lines.append("Current Assets:")
        lines.append(f"  Cash and Cash Equivalents                {self._format_currency(cash):>20}")
        lines.append(f"  Accounts Receivable, Net                 {self._format_currency(accounts_receivable):>20}")
        if inventory > 0:
            lines.append(f"  Inventory                                {self._format_currency(inventory):>20}")
        lines.append(f"  Prepaid Expenses                         {self._format_currency(prepaid_expenses):>20}")
        lines.append("-" * 70)
        lines.append(f"Total Current Assets                       {self._format_currency(current_assets):>20}")
        lines.append("")
        
        lines.append("Non-Current Assets:")
        lines.append(f"  Property, Plant and Equipment, Net       {self._format_currency(ppe):>20}")
        lines.append(f"  Intangible Assets                        {self._format_currency(intangible_assets):>20}")
        lines.append(f"  Long-term Investments                    {self._format_currency(investments):>20}")
        lines.append(f"  Other Assets                             {self._format_currency(other_assets):>20}")
        lines.append("-" * 70)
        lines.append(f"Total Non-Current Assets                   {self._format_currency(ppe + intangible_assets + investments + other_assets):>20}")
        lines.append("=" * 70)
        lines.append(f"TOTAL ASSETS                               {self._format_currency(total_assets):>20}")
        lines.append("=" * 70)
        lines.append("")
        
        lines.append("LIABILITIES")
        lines.append("-" * 70)
        lines.append("Current Liabilities:")
        lines.append(f"  Accounts Payable                         {self._format_currency(accounts_payable):>20}")
        lines.append(f"  Accrued Expenses                         {self._format_currency(accrued_expenses):>20}")
        lines.append(f"  Short-term Debt                          {self._format_currency(short_term_debt):>20}")
        lines.append(f"  Current Portion of Long-term Debt        {self._format_currency(current_portion_lt_debt):>20}")
        lines.append("-" * 70)
        lines.append(f"Total Current Liabilities                  {self._format_currency(current_liabilities):>20}")
        lines.append("")
        
        lines.append("Non-Current Liabilities:")
        lines.append(f"  Long-term Debt                           {self._format_currency(long_term_debt):>20}")
        lines.append(f"  Deferred Tax Liability                   {self._format_currency(deferred_tax):>20}")
        lines.append(f"  Other Liabilities                        {self._format_currency(other_liabilities):>20}")
        lines.append("-" * 70)
        lines.append(f"Total Non-Current Liabilities              {self._format_currency(long_term_debt + deferred_tax + other_liabilities):>20}")
        lines.append("=" * 70)
        lines.append(f"TOTAL LIABILITIES                          {self._format_currency(total_liabilities):>20}")
        lines.append("=" * 70)
        lines.append("")
        
        lines.append("STOCKHOLDERS' EQUITY")
        lines.append("-" * 70)
        lines.append(f"  Common Stock                             {self._format_currency(common_stock):>20}")
        lines.append(f"  Additional Paid-in Capital               {self._format_currency(additional_paid_in):>20}")
        lines.append(f"  Retained Earnings                        {self._format_currency(retained_earnings):>20}")
        lines.append("-" * 70)
        lines.append(f"Total Stockholders' Equity                 {self._format_currency(total_equity):>20}")
        lines.append("=" * 70)
        
        total_liab_equity = total_liabilities + total_equity
        lines.append(f"TOTAL LIABILITIES AND EQUITY               {self._format_currency(total_liab_equity):>20}")
        lines.append("=" * 70)
        
        if add_discrepancy:
            lines.append("")
            lines.append(f"Note: Equity discrepancy of {self._format_currency(abs(discrepancy))}")
        
        return "\n".join(lines)
    
    def _generate_proposal_content(
        self,
        company: Company,
        application_id: str,
        loan_amount: float
    ) -> str:
        """Generate loan application proposal PDF content."""
        
        lines = []
        lines.append("=" * 70)
        lines.append("           LOAN APPLICATION PROPOSAL")
        lines.append("=" * 70)
        lines.append("")
        lines.append(f"Application ID: {application_id}")
        lines.append(f"Date: {datetime.now().strftime('%B %d, %Y')}")
        lines.append("")
        lines.append("-" * 70)
        lines.append("APPLICANT INFORMATION")
        lines.append("-" * 70)
        lines.append(f"Company Name:     {company.name}")
        lines.append(f"Industry:         {company.industry}")
        lines.append(f"Tax ID:           {company.tax_id}")
        lines.append(f"Incorporated:     {company.incorporation_date.strftime('%B %d, %Y')}")
        lines.append(f"Employees:        {company.employee_count:,}")
        lines.append("")
        lines.append(f"Address:          {company.address}")
        lines.append(f"                  {company.city}, {company.state} {company.zip_code}")
        lines.append(f"Phone:            {company.phone}")
        lines.append(f"Email:            {company.email}")
        lines.append("")
        lines.append("-" * 70)
        lines.append("LOAN REQUEST DETAILS")
        lines.append("-" * 70)
        lines.append(f"Requested Amount: {self._format_currency(loan_amount)}")
        lines.append(f"Purpose:          Working Capital / Expansion")
        lines.append(f"Term:             60 months")
        lines.append(f"Proposed Rate:    {self.rng.uniform(4.5, 9.5):.2f}%")
        lines.append("")
        lines.append("-" * 70)
        lines.append("FINANCIAL SUMMARY")
        lines.append("-" * 70)
        
        latest = company.financial_history[-1] if company.financial_history else None
        if latest:
            lines.append(f"Annual Revenue:   {self._format_currency(latest.revenue)}")
            lines.append(f"Net Income:       {self._format_currency(latest.net_income)}")
            lines.append(f"Total Assets:     {self._format_currency(latest.total_assets)}")
            lines.append(f"EBITDA:           {self._format_currency(latest.ebitda)}")
        
        lines.append("")
        lines.append("-" * 70)
        lines.append("RISK ASSESSMENT")
        lines.append("-" * 70)
        lines.append(f"Company Profile:  {company.profile_type.value}")
        lines.append(f"Risk Level:       {company.risk_level.value}")
        
        if company.compliance_flags:
            lines.append("")
            lines.append("Active Compliance Flags:")
            for flag in company.compliance_flags:
                if flag.resolved_at is None:
                    lines.append(f"  - [{flag.severity}] {flag.flag_type}")
        
        lines.append("")
        lines.append("=" * 70)
        lines.append("This proposal is subject to credit approval and final due diligence.")
        lines.append("=" * 70)
        
        return "\n".join(lines)
    
    def generate_income_statements(
        self,
        companies: list[Company],
        output_dir: Path
    ) -> list[Path]:
        """Generate income statement PDFs for all companies."""
        generated = []
        
        # Calculate how many of each variant
        variant_assignments = []
        for variant in INCOME_STATEMENT_VARIANTS:
            variant_assignments.extend([variant.name] * variant.count)
        
        # Shuffle to distribute randomly
        self.rng.shuffle(variant_assignments)
        
        for i, company in enumerate(companies):
            if i >= len(variant_assignments):
                break
            
            variant = variant_assignments[i]
            latest_financials = company.financial_history[-1] if company.financial_history else None
            
            if latest_financials:
                content = self._generate_income_statement_content(company, latest_financials, variant)
                
                # Create company directory
                company_dir = output_dir / "documents" / str(company.company_id)
                company_dir.mkdir(parents=True, exist_ok=True)
                
                # Write text file (simulating PDF content)
                file_path = company_dir / f"income_statement_2024_{variant}.txt"
                file_path.write_text(content)
                generated.append(file_path)
                self.generated_files.append(file_path)
        
        return generated
    
    def generate_balance_sheets(
        self,
        companies: list[Company],
        output_dir: Path,
        discrepancy_count: int = 6
    ) -> list[Path]:
        """Generate balance sheet PDFs with optional discrepancies."""
        generated = []
        
        # Select companies for discrepancies
        discrepancy_indices = set(self.rng.sample(range(len(companies)), min(discrepancy_count, len(companies))))
        
        for i, company in enumerate(companies):
            latest_financials = company.financial_history[-1] if company.financial_history else None
            
            if latest_financials:
                add_discrepancy = i in discrepancy_indices
                content = self._generate_balance_sheet_content(company, latest_financials, add_discrepancy)
                
                # Create company directory
                company_dir = output_dir / "documents" / str(company.company_id)
                company_dir.mkdir(parents=True, exist_ok=True)
                
                suffix = "_discrepancy" if add_discrepancy else ""
                file_path = company_dir / f"balance_sheet_2024{suffix}.txt"
                file_path.write_text(content)
                generated.append(file_path)
                self.generated_files.append(file_path)
        
        return generated
    
    def generate_proposals(
        self,
        companies: list[Company],
        applications: list[dict[str, Any]],
        output_dir: Path
    ) -> list[Path]:
        """Generate loan application proposal PDFs."""
        generated = []
        
        for app in applications:
            company_id = app.get("company_id")
            company = next((c for c in companies if str(c.company_id) == str(company_id)), None)
            
            if company:
                content = self._generate_proposal_content(
                    company,
                    app["application_id"],
                    app.get("loan_amount", 5_000_000)
                )
                
                # Create company directory
                company_dir = output_dir / "documents" / str(company.company_id)
                company_dir.mkdir(parents=True, exist_ok=True)
                
                file_path = company_dir / f"application_proposal_{app['application_id']}.txt"
                file_path.write_text(content)
                generated.append(file_path)
                self.generated_files.append(file_path)
        
        return generated
    
    def generate_all_pdfs(
        self,
        companies: list[Company],
        applications: list[dict[str, Any]],
        output_dir: Path
    ) -> dict[str, list[Path]]:
        """Generate all PDF documents."""
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        return {
            "income_statements": self.generate_income_statements(companies, output_dir),
            "balance_sheets": self.generate_balance_sheets(companies, output_dir),
            "proposals": self.generate_proposals(companies, applications, output_dir),
        }


if __name__ == "__main__":
    from company_generator import generate_companies
    
    companies = generate_companies(80, random_seed=42)
    
    # Mock applications for testing
    applications = [
        {
            "application_id": f"APEX-{i+1:04d}",
            "company_id": companies[i % len(companies)].company_id,
            "loan_amount": 5_000_000
        }
        for i in range(29)
    ]
    
    generator = PDFGenerator(random_seed=42)
    output_dir = Path("./test_output")
    
    results = generator.generate_all_pdfs(companies, applications, output_dir)
    
    print("Generated PDFs:")
    for category, files in results.items():
        print(f"  {category}: {len(files)} files")
