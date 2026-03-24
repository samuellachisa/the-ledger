"""
Company profile generator for applicant_registry schema.

Generates 80 company profiles with:
- Distribution: GROWTH:20, STABLE:25, DECLINING:12, RECOVERING:13, VOLATILE:10
- Risk: LOW:24, MEDIUM:33, HIGH:23
- Compliance flags: 8 companies with active flags
- Financial history (3 years)
- Loan relationships
"""
from __future__ import annotations

import random
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any
from uuid import UUID, uuid4


class CompanyProfile(str, Enum):
    GROWTH = "GROWTH"
    STABLE = "STABLE"
    DECLINING = "DECLINING"
    RECOVERING = "RECOVERING"
    VOLATILE = "VOLATILE"


class RiskLevel(str, Enum):
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"


@dataclass
class FinancialHistory:
    year: int
    revenue: float
    net_income: float
    total_assets: float
    total_liabilities: float
    ebitda: float
    operating_cash_flow: float


@dataclass
class ComplianceFlag:
    flag_id: UUID
    company_id: UUID
    flag_type: str
    severity: str
    description: str
    created_at: datetime
    resolved_at: datetime | None


@dataclass
class LoanRelationship:
    relationship_id: UUID
    company_id: UUID
    lender_name: str
    loan_amount: float
    interest_rate: float
    start_date: datetime
    maturity_date: datetime
    status: str


@dataclass
class Company:
    company_id: UUID
    name: str
    industry: str
    incorporation_date: datetime
    employee_count: int
    annual_revenue: float
    profile_type: CompanyProfile
    risk_level: RiskLevel
    address: str
    city: str
    state: str
    zip_code: str
    phone: str
    email: str
    tax_id: str
    created_at: datetime
    updated_at: datetime
    financial_history: list[FinancialHistory] = field(default_factory=list)
    compliance_flags: list[ComplianceFlag] = field(default_factory=list)
    loan_relationships: list[LoanRelationship] = field(default_factory=list)


# Industry distribution
INDUSTRIES = [
    "Technology", "Healthcare", "Manufacturing", "Retail", "Financial Services",
    "Energy", "Real Estate", "Transportation", "Agriculture", "Construction",
    "Education", "Hospitality", "Media", "Telecommunications", "Consulting"
]

# Company name components
COMPANY_PREFIXES = ["Advanced", "Global", "United", "Premier", "Summit", "Peak", 
                    "First", "National", "American", "Pacific", "Atlantic", "Metro"]
COMPANY_SUFFIXES = ["Corp", "Inc", "Ltd", "Solutions", "Group", "Holdings", 
                    "Partners", "Enterprises", "Systems", "Technologies", "Services"]
COMPANY_NOUNS = ["Data", "Tech", "Finance", "Health", "Energy", "Logistics", 
                 "Media", "Retail", "Manufacturing", "Construction", "Consulting"]

# US States for distribution
US_STATES = [
    ("CA", "California"), ("TX", "Texas"), ("NY", "New York"), ("FL", "Florida"),
    ("IL", "Illinois"), ("PA", "Pennsylvania"), ("OH", "Ohio"), ("GA", "Georgia"),
    ("NC", "North Carolina"), ("MI", "Michigan"), ("NJ", "New Jersey"), ("VA", "Virginia"),
    ("WA", "Washington"), ("AZ", "Arizona"), ("MA", "Massachusetts"), ("TN", "Tennessee"),
    ("IN", "Indiana"), ("MO", "Missouri"), ("MD", "Maryland"), ("WI", "Wisconsin"),
    ("CO", "Colorado"), ("MN", "Minnesota"), ("SC", "South Carolina"), ("AL", "Alabama"),
    ("LA", "Louisiana"), ("KY", "Kentucky"), ("OR", "Oregon"), ("OK", "Oklahoma"),
    ("CT", "Connecticut"), ("UT", "Utah"), ("IA", "Iowa"), ("NV", "Nevada"),
    ("AR", "Arkansas"), ("MS", "Mississippi"), ("KS", "Kansas"), ("NM", "New Mexico"),
    ("NE", "Nebraska"), ("WV", "West Virginia"), ("ID", "Idaho"), ("HI", "Hawaii"),
    ("NH", "New Hampshire"), ("ME", "Maine"), ("MT", "Montana"), ("RI", "Rhode Island"),
    ("DE", "Delaware"), ("SD", "South Dakota"), ("ND", "North Dakota"), ("AK", "Alaska"),
    ("VT", "Vermont"), ("WY", "Wyoming")
]

# Compliance flag types
COMPLIANCE_FLAG_TYPES = [
    ("REGULATORY_FILING_OVERDUE", "MEDIUM", "Annual regulatory filing overdue"),
    ("AUDIT_FINDING", "HIGH", "Significant audit finding identified"),
    ("LITIGATION_PENDING", "MEDIUM", "Active litigation case pending"),
    ("SANCTIONS_SCREENING", "HIGH", "Sanctions list screening match"),
    ("KYC_REVIEW_REQUIRED", "LOW", "Know Your Customer review required"),
    ("ENVIRONMENTAL_VIOLATION", "HIGH", "Environmental compliance violation"),
]

# Lender names
LENDER_NAMES = [
    "First National Bank", "Capital Trust", "Summit Financial", "Metro Credit Union",
    "Pacific Lending Group", "Atlantic Business Finance", "United Commercial Bank",
    "Heritage Capital", "Premier Funding Corp", "Sterling Bank & Trust"
]


def generate_company_name(rng: random.Random) -> str:
    """Generate a realistic company name."""
    pattern = rng.randint(0, 3)
    if pattern == 0:
        return f"{rng.choice(COMPANY_PREFIXES)} {rng.choice(COMPANY_NOUNS)} {rng.choice(COMPANY_SUFFIXES)}"
    elif pattern == 1:
        return f"{rng.choice(COMPANY_NOUNS)} {rng.choice(COMPANY_SUFFIXES)}"
    elif pattern == 2:
        return f"{rng.choice(COMPANY_PREFIXES)} {rng.choice(INDUSTRIES)} {rng.choice(COMPANY_SUFFIXES)}"
    else:
        noun = rng.choice(COMPANY_NOUNS)
        return f"{noun}{rng.choice(['', 's'])} {rng.choice(COMPANY_SUFFIXES)}"


def generate_tax_id(rng: random.Random) -> str:
    """Generate a valid-format tax ID (XX-XXXXXXX)."""
    return f"{rng.randint(10, 99)}-{rng.randint(1000000, 9999999)}"


def generate_phone(rng: random.Random) -> str:
    """Generate a valid-format US phone number."""
    area = rng.randint(200, 999)
    prefix = rng.randint(200, 999)
    line = rng.randint(0, 9999)
    return f"({area}) {prefix:03d}-{line:04d}"


def generate_financial_history(
    company_id: UUID,
    profile_type: CompanyProfile,
    base_revenue: float,
    rng: random.Random
) -> list[FinancialHistory]:
    """Generate 3 years of financial history based on company profile."""
    history = []
    current_year = datetime.now().year
    
    # Profile-based growth/change rates
    growth_rates = {
        CompanyProfile.GROWTH: (0.15, 0.35),
        CompanyProfile.STABLE: (-0.05, 0.08),
        CompanyProfile.DECLINING: (-0.25, -0.05),
        CompanyProfile.RECOVERING: (-0.10, 0.25),
        CompanyProfile.VOLATILE: (-0.20, 0.30),
    }
    
    min_rate, max_rate = growth_rates[profile_type]
    
    for i, year in enumerate(range(current_year - 3, current_year)):
        if i == 0:
            revenue = base_revenue
        else:
            growth = rng.uniform(min_rate, max_rate)
            revenue = history[-1].revenue * (1 + growth)
        
        # Profit margins vary by profile
        margin_ranges = {
            CompanyProfile.GROWTH: (0.08, 0.18),
            CompanyProfile.STABLE: (0.10, 0.15),
            CompanyProfile.DECLINING: (-0.05, 0.05),
            CompanyProfile.RECOVERING: (0.02, 0.12),
            CompanyProfile.VOLATILE: (-0.08, 0.20),
        }
        margin = rng.uniform(*margin_ranges[profile_type])
        net_income = revenue * margin
        
        # Assets and liabilities
        asset_turnover = rng.uniform(0.8, 1.5)
        total_assets = revenue / asset_turnover
        debt_ratio = rng.uniform(0.3, 0.7)
        total_liabilities = total_assets * debt_ratio
        
        # EBITDA
        ebitda_margin = rng.uniform(0.12, 0.25)
        ebitda = revenue * ebitda_margin
        
        # Operating cash flow
        ocf_ratio = rng.uniform(0.6, 1.0)
        operating_cash_flow = ebitda * ocf_ratio
        
        history.append(FinancialHistory(
            year=year,
            revenue=round(revenue, 2),
            net_income=round(net_income, 2),
            total_assets=round(total_assets, 2),
            total_liabilities=round(total_liabilities, 2),
            ebitda=round(ebitda, 2),
            operating_cash_flow=round(operating_cash_flow, 2)
        ))
    
    return history


def generate_compliance_flags(
    company_id: UUID,
    count: int,
    rng: random.Random
) -> list[ComplianceFlag]:
    """Generate compliance flags for a company."""
    flags = []
    now = datetime.now()
    
    for _ in range(count):
        flag_type, severity, description = rng.choice(COMPLIANCE_FLAG_TYPES)
        created_at = now - timedelta(days=rng.randint(30, 365))
        
        # 30% chance flag is resolved
        resolved_at = None
        if rng.random() < 0.3:
            resolved_at = created_at + timedelta(days=rng.randint(15, 180))
        
        flags.append(ComplianceFlag(
            flag_id=uuid4(),
            company_id=company_id,
            flag_type=flag_type,
            severity=severity,
            description=description,
            created_at=created_at,
            resolved_at=resolved_at
        ))
    
    return flags


def generate_loan_relationships(
    company_id: UUID,
    count: int,
    rng: random.Random
) -> list[LoanRelationship]:
    """Generate existing loan relationships for a company."""
    relationships = []
    now = datetime.now()
    
    for _ in range(count):
        lender = rng.choice(LENDER_NAMES)
        loan_amount = rng.choice([500000, 1000000, 2500000, 5000000, 10000000])
        interest_rate = round(rng.uniform(0.035, 0.095), 4)
        
        start_date = now - timedelta(days=rng.randint(180, 1095))
        maturity_date = start_date + timedelta(days=rng.randint(365, 1825))
        
        # Determine status based on dates
        if maturity_date < now:
            status = rng.choice(["PAID_OFF", "DEFAULTED"])
        else:
            status = rng.choice(["ACTIVE", "ACTIVE", "ACTIVE", "REFINANCING"])
        
        relationships.append(LoanRelationship(
            relationship_id=uuid4(),
            company_id=company_id,
            lender_name=lender,
            loan_amount=loan_amount,
            interest_rate=interest_rate,
            start_date=start_date,
            maturity_date=maturity_date,
            status=status
        ))
    
    return relationships


def generate_companies(
    count: int = 80,
    random_seed: int = 42
) -> list[Company]:
    """
    Generate company profiles with specified distributions.
    
    Distribution:
    - GROWTH: 20, STABLE: 25, DECLINING: 12, RECOVERING: 13, VOLATILE: 10
    - Risk LOW: 24, MEDIUM: 33, HIGH: 23
    - 8 companies with active compliance flags
    """
    rng = random.Random(random_seed)
    companies = []
    
    # Profile distribution
    profile_distribution = [
        (CompanyProfile.GROWTH, 20),
        (CompanyProfile.STABLE, 25),
        (CompanyProfile.DECLINING, 12),
        (CompanyProfile.RECOVERING, 13),
        (CompanyProfile.VOLATILE, 10),
    ]
    
    # Risk distribution
    risk_distribution = [
        (RiskLevel.LOW, 24),
        (RiskLevel.MEDIUM, 33),
        (RiskLevel.HIGH, 23),
    ]
    
    # Create profile list
    profiles = []
    for profile, num in profile_distribution:
        profiles.extend([profile] * num)
    rng.shuffle(profiles)
    
    # Create risk list
    risks = []
    for risk, num in risk_distribution:
        risks.extend([risk] * num)
    rng.shuffle(risks)
    
    # Select 8 companies for compliance flags
    flagged_indices = set(rng.sample(range(count), 8))
    
    now = datetime.now()
    
    for i in range(count):
        company_id = uuid4()
        profile = profiles[i]
        risk = risks[i]
        
        # Generate base company info
        name = generate_company_name(rng)
        industry = rng.choice(INDUSTRIES)
        
        # Incorporation date based on profile
        if profile == CompanyProfile.GROWTH:
            years_ago = rng.randint(2, 10)
        elif profile == CompanyProfile.STABLE:
            years_ago = rng.randint(10, 30)
        else:
            years_ago = rng.randint(5, 25)
        incorporation_date = now - timedelta(days=years_ago * 365 + rng.randint(0, 365))
        
        # Employee count based on profile
        if profile == CompanyProfile.GROWTH:
            employee_count = rng.randint(50, 500)
        elif profile == CompanyProfile.STABLE:
            employee_count = rng.randint(200, 2000)
        elif profile == CompanyProfile.DECLINING:
            employee_count = rng.randint(100, 1000)
        else:
            employee_count = rng.randint(20, 500)
        
        # Base annual revenue
        base_revenue = rng.uniform(5_000_000, 100_000_000)
        
        # Address
        state_code, state_name = rng.choice(US_STATES)
        city = rng.choice(["Springfield", "Riverside", "Fairview", "Madison", "Clayton",
                          "Georgetown", "Salem", "Greenville", "Clinton", "Franklin"])
        address = f"{rng.randint(100, 9999)} {rng.choice(['Main', 'Oak', 'Park', 'Market', 'Broad'])} {rng.choice(['St', 'Ave', 'Blvd', 'Dr'])}"
        zip_code = f"{rng.randint(10000, 99999)}"
        
        # Contact info
        phone = generate_phone(rng)
        email = f"info@{name.lower().replace(' ', '').replace('&', 'and')}.com"
        tax_id = generate_tax_id(rng)
        
        # Generate financial history
        financial_history = generate_financial_history(company_id, profile, base_revenue, rng)
        latest_revenue = financial_history[-1].revenue
        
        # Generate compliance flags (only for flagged companies)
        compliance_flags = []
        if i in flagged_indices:
            flag_count = rng.randint(1, 2)
            compliance_flags = generate_compliance_flags(company_id, flag_count, rng)
        
        # Generate loan relationships (0-3 per company)
        loan_count = rng.randint(0, 3)
        loan_relationships = generate_loan_relationships(company_id, loan_count, rng)
        
        company = Company(
            company_id=company_id,
            name=name,
            industry=industry,
            incorporation_date=incorporation_date,
            employee_count=employee_count,
            annual_revenue=round(latest_revenue, 2),
            profile_type=profile,
            risk_level=risk,
            address=address,
            city=city,
            state=state_code,
            zip_code=zip_code,
            phone=phone,
            email=email,
            tax_id=tax_id,
            created_at=now - timedelta(days=rng.randint(30, 365)),
            updated_at=now,
            financial_history=financial_history,
            compliance_flags=compliance_flags,
            loan_relationships=loan_relationships
        )
        companies.append(company)
    
    return companies


def to_database_records(companies: list[Company]) -> dict[str, list[dict[str, Any]]]:
    """Convert companies to flat database record dictionaries."""
    records = {
        "companies": [],
        "financial_history": [],
        "compliance_flags": [],
        "loan_relationships": []
    }
    
    for company in companies:
        # Company record
        records["companies"].append({
            "company_id": company.company_id,
            "name": company.name,
            "industry": company.industry,
            "incorporation_date": company.incorporation_date,
            "employee_count": company.employee_count,
            "annual_revenue": company.annual_revenue,
            "profile_type": company.profile_type.value,
            "risk_level": company.risk_level.value,
            "address": company.address,
            "city": company.city,
            "state": company.state,
            "zip_code": company.zip_code,
            "phone": company.phone,
            "email": company.email,
            "tax_id": company.tax_id,
            "created_at": company.created_at,
            "updated_at": company.updated_at,
        })
        
        # Financial history records
        for fh in company.financial_history:
            records["financial_history"].append({
                "history_id": uuid4(),
                "company_id": company.company_id,
                "year": fh.year,
                "revenue": fh.revenue,
                "net_income": fh.net_income,
                "total_assets": fh.total_assets,
                "total_liabilities": fh.total_liabilities,
                "ebitda": fh.ebitda,
                "operating_cash_flow": fh.operating_cash_flow,
            })
        
        # Compliance flags
        for cf in company.compliance_flags:
            records["compliance_flags"].append({
                "flag_id": cf.flag_id,
                "company_id": cf.company_id,
                "flag_type": cf.flag_type,
                "severity": cf.severity,
                "description": cf.description,
                "created_at": cf.created_at,
                "resolved_at": cf.resolved_at,
            })
        
        # Loan relationships
        for lr in company.loan_relationships:
            records["loan_relationships"].append({
                "relationship_id": lr.relationship_id,
                "company_id": lr.company_id,
                "lender_name": lr.lender_name,
                "loan_amount": lr.loan_amount,
                "interest_rate": lr.interest_rate,
                "start_date": lr.start_date,
                "maturity_date": lr.maturity_date,
                "status": lr.status,
            })
    
    return records


if __name__ == "__main__":
    # Test generation
    companies = generate_companies(80, random_seed=42)
    print(f"Generated {len(companies)} companies")
    
    # Print distribution
    profile_counts = {}
    risk_counts = {}
    total_flags = 0
    total_loans = 0
    
    for c in companies:
        profile_counts[c.profile_type.value] = profile_counts.get(c.profile_type.value, 0) + 1
        risk_counts[c.risk_level.value] = risk_counts.get(c.risk_level.value, 0) + 1
        total_flags += len(c.compliance_flags)
        total_loans += len(c.loan_relationships)
    
    print(f"\nProfile distribution: {profile_counts}")
    print(f"Risk distribution: {risk_counts}")
    print(f"Companies with flags: {sum(1 for c in companies if c.compliance_flags)}")
    print(f"Total compliance flags: {total_flags}")
    print(f"Total loan relationships: {total_loans}")
    
    # Show sample company
    sample = companies[0]
    print(f"\nSample company: {sample.name}")
    print(f"  Profile: {sample.profile_type.value}, Risk: {sample.risk_level.value}")
    print(f"  Revenue: ${sample.annual_revenue:,.2f}")
    print(f"  Financial history years: {len(sample.financial_history)}")
