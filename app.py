import os
import smtplib
import socket
import dns.resolver
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from email_validator import validate_email, EmailNotValidError
import streamlit as st
import re
from urllib.parse import urlparse

# --------------------------
# ✅ CONFIG
# --------------------------
MAX_THREADS = 5
FROM_EMAIL = "test@example.com"
MX_CACHE = {}

ROLE_ACCOUNTS = {"admin", "info", "support", "contact", "sales"}
DISPOSABLE_DOMAINS = {"mailinator.com", "tempmail.com", "10minutemail.com"}
FREE_PROVIDERS = {"gmail.com", "yahoo.com", "outlook.com", "hotmail.com"}

os.makedirs("uploads", exist_ok=True)

# --------------------------
# ✅ LOGGING
# --------------------------
def log_error(msg):
    with open("logs.txt", "a") as f:
        f.write(msg + "\n")

# --------------------------
# ✅ SMTP HANDSHAKE
# --------------------------
def smtp_check(email):
    domain = email.split('@')[1]
    try:
        if domain in MX_CACHE:
            mx_record = MX_CACHE[domain]
        else:
            answers = dns.resolver.resolve(domain, 'MX', lifetime=5)
            mx_record = str(answers[0].exchange).rstrip('.')
            MX_CACHE[domain] = mx_record

        server = smtplib.SMTP(timeout=5)
        server.connect(mx_record)
        server.helo(socket.gethostname())
        server.mail(FROM_EMAIL)
        code, _ = server.rcpt(email)
        server.quit()
        return code
    except Exception as e:
        log_error(f"SMTP error for {email}: {e}")
        return None

def is_catch_all(domain):
    fake_email = f"notareal12345@{domain}"
    result = smtp_check(fake_email)
    return result == 250

# --------------------------
# ✅ VERIFY ONE EMAIL
# --------------------------
def verify_email_verbose(email):
    email = email.strip().lower()
    steps = []

    syntax_result = '✅'
    role_result = '✅'
    mx_result = '✅'
    smtp_result = '✅'
    catchall_result = '✅'

    try:
        valid = validate_email(email, check_deliverability=True)
        email = valid.email
        steps.append("✅ Syntax & domain DNS: OK")
    except EmailNotValidError as e:
        steps.append(f"❌ Syntax/DNS error: {e}")
        syntax_result = '❌'
        return "Invalid", steps, syntax_result, role_result, mx_result, smtp_result, catchall_result

    local, domain = email.split('@')

    if local in ROLE_ACCOUNTS:
        steps.append("⚠️ Role-based: Risky")
        role_result = '⚠️'
        return "Risky", steps, syntax_result, role_result, mx_result, smtp_result, catchall_result

    if domain in DISPOSABLE_DOMAINS:
        steps.append("⚠️ Disposable domain: Risky")
        role_result = '⚠️'
        return "Risky", steps, syntax_result, role_result, mx_result, smtp_result, catchall_result

    try:
        answers = dns.resolver.resolve(domain, 'MX', lifetime=5)
    except Exception as e:
        steps.append(f"❌ MX check failed: {e}")
        mx_result = '❌'
        return "Invalid", steps, syntax_result, role_result, mx_result, smtp_result, catchall_result

    code = smtp_check(email)
    if code == 250:
        if domain in FREE_PROVIDERS:
            steps.append("✅ Free provider with mailbox exists.")
            return "Valid", steps, syntax_result, role_result, mx_result, smtp_result, catchall_result
        if is_catch_all(domain):
            steps.append("⚠️ Catch-all domain detected")
            catchall_result = '⚠️'
            return "Risky", steps, syntax_result, role_result, mx_result, smtp_result, catchall_result
        steps.append("✅ SMTP: Mailbox exists")
        return "Valid", steps, syntax_result, role_result, mx_result, smtp_result, catchall_result
    elif code == 550:
        steps.append("❌ SMTP: Mailbox invalid (550)")
        smtp_result = '❌'
        return "Invalid", steps, syntax_result, role_result, mx_result, smtp_result, catchall_result
    else:
        steps.append(f"⚠️ SMTP: Greylisted or uncertain")
        smtp_result = '⚠️'
        return "Risky", steps, syntax_result, role_result, mx_result, smtp_result, catchall_result

# --------------------------
# ✅ EXPAND MULTIPLE EMAILS
# --------------------------
def expand_rows(df, email_col):
    rows = []
    for _, row in df.iterrows():
        emails = str(row[email_col]).split(',')
        for email in emails:
            email = email.strip()
            if email and email != "-- No Data --":
                new_row = row.copy()
                new_row[email_col] = email
                rows.append(new_row)
    return pd.DataFrame(rows)

# --------------------------
# ✅ EXTRACT DOMAIN FROM WEBSITE URL
# --------------------------
def extract_domain_from_website(website_url):
    """
    Extract main domain from website URL.
    Removes https://, http://, www, and path components.
    Returns None if invalid or empty.
    """
    if pd.isna(website_url) or not website_url or str(website_url).strip() == "":
        return None
    
    website_url = str(website_url).strip()
    
    # Remove protocol if present
    if website_url.startswith(('http://', 'https://')):
        parsed = urlparse(website_url)
        domain = parsed.netloc or parsed.path
    else:
        domain = website_url
    
    # Remove www. prefix
    domain = re.sub(r'^www\.', '', domain, flags=re.IGNORECASE)
    
    # Remove path and query parameters
    domain = domain.split('/')[0].split('?')[0].split('#')[0]
    
    # Remove port if present
    domain = domain.split(':')[0]
    
    # Clean up and validate
    domain = domain.strip().lower()
    
    if not domain or domain == "-- no data --" or domain == "nan":
        return None
    
    return domain

# --------------------------
# ✅ CHECK IF EMAIL MATCHES ALLOWED DOMAINS
# --------------------------
def is_email_allowed(email, website_domain=None):
    """
    Check if email domain is allowed:
    - @gmail.com
    - @outlook.com
    - @yahoo.* (any yahoo domain)
    - OR matches the business's own domain (from website)
    """
    if pd.isna(email) or not email or str(email).strip() == "":
        return False
    
    email = str(email).strip().lower()
    
    # Extract email domain
    if '@' not in email:
        return False
    
    email_domain = email.split('@')[1]
    
    # Check for allowed free providers
    if email_domain == 'gmail.com':
        return True
    
    if email_domain == 'outlook.com':
        return True
    
    # Check for any yahoo domain (yahoo.com, yahoo.com.au, etc.)
    if email_domain.startswith('yahoo.'):
        return True
    
    # Check if email matches business domain
    if website_domain:
        if email_domain == website_domain:
            return True
    
    return False

# --------------------------
# ✅ FILTER LEADS BY EMAIL DOMAIN
# --------------------------
def filter_leads_by_domain(df, email_col, website_col=None):
    """
    Filter leads to keep only:
    - Emails from @gmail.com, @outlook.com, @yahoo.*
    - OR emails matching the business domain (from Website column)
    """
    if len(df) == 0:
        return df
    
    filtered_rows = []
    
    for idx, row in df.iterrows():
        email = row[email_col]
        
        # Skip if email is empty or invalid
        if pd.isna(email) or not email or str(email).strip() == "":
            continue
        
        # Extract website domain if Website column exists
        website_domain = None
        if website_col and website_col in df.columns:
            website_url = row[website_col]
            website_domain = extract_domain_from_website(website_url)
        
        # Check if email is allowed
        if is_email_allowed(email, website_domain):
            filtered_rows.append(row)
    
    return pd.DataFrame(filtered_rows).reset_index(drop=True)

# --------------------------
# ✅ FILTER LEADS BY EXCLUDED NAMES
# --------------------------
def filter_leads_by_excluded_names(df, excluded_names, name_col=None):
    """
    Remove leads where the name matches any of the excluded names.
    Case-insensitive matching.
    """
    if len(df) == 0 or not excluded_names or len(excluded_names) == 0:
        return df
    
    if not name_col or name_col not in df.columns:
        return df
    
    # Normalize excluded names (lowercase, strip)
    excluded_names_normalized = [name.strip().lower() for name in excluded_names if name.strip()]
    
    if not excluded_names_normalized:
        return df
    
    filtered_rows = []
    
    for idx, row in df.iterrows():
        name = row[name_col]
        
        # Skip if name is empty
        if pd.isna(name) or not name:
            filtered_rows.append(row)
            continue
        
        # Normalize the lead's name
        name_normalized = str(name).strip().lower()
        
        # Check if name matches any excluded name
        should_exclude = False
        for excluded_name in excluded_names_normalized:
            # Check for exact match or if excluded name is contained in the lead's name
            if excluded_name == name_normalized or excluded_name in name_normalized:
                should_exclude = True
                break
        
        # Keep the lead if it doesn't match any excluded name
        if not should_exclude:
            filtered_rows.append(row)
    
    return pd.DataFrame(filtered_rows).reset_index(drop=True)

# --------------------------
# ✅ PROCESS FILE
# --------------------------
def process_file(uploaded_file, progress_area, table_area, final_summary_area, excluded_names=None):
    df = pd.read_csv(uploaded_file)

    # ✅ REMOVE UNNECESSARY FIELDS
    fields_to_remove = [
        "ID", "Featured image", "Bing Maps URL", "Latitude", "Longitude",
        "Rating", "Rating Info", "Open Hours", "Price", "Scraped At"
    ]
    df.drop(columns=[col for col in fields_to_remove if col in df.columns], inplace=True)
    st.info("✅ Unnecessary fields removed")

    email_col = next((col for col in df.columns if 'email' in col.lower()), None)
    if not email_col:
        raise Exception(f"No email column found. Columns: {df.columns.tolist()}")

    df = expand_rows(df, email_col)
    st.info("✅ Step 1: Email splitting done")

    df = df[df[email_col] != ""].reset_index(drop=True)
    st.info("✅ Step 2: '-- No Data --' rows removed")

    total = len(df)
    completed = 0
    valid_count = 0
    risky_count = 0
    invalid_count = 0

    results_df = pd.DataFrame(columns=[
        'Email', 'Syntax', 'Role/Disposable', 'MX', 'SMTP', 'Catch-All', 'Status'
    ])

    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        futures = {executor.submit(verify_email_verbose, email): i for i, email in enumerate(df[email_col])}
        for future in as_completed(futures):
            i = futures[future]
            email = df.loc[i, email_col]

            try:
                status, steps, syntax_result, role_result, mx_result, smtp_result, catchall_result = future.result()
            except Exception as e:
                status = "Risky"
                syntax_result = role_result = mx_result = smtp_result = catchall_result = '⚠️'
                log_error(f"Error for {email}: {e}")

            completed += 1
            df.loc[i, 'Email Status'] = status

            if status == "Valid": valid_count += 1
            elif status == "Risky": risky_count += 1
            elif status == "Invalid": invalid_count += 1

            results_df.loc[len(results_df)] = [
                email, syntax_result, role_result, mx_result, smtp_result, catchall_result, status
            ]

            progress_area.progress(completed / total)
            table_area.dataframe(results_df)

    st.info("✅ Step 3: Email verification done")

    df = df[df['Email Status'] != 'Invalid'].reset_index(drop=True)
    st.info("✅ Step 4: Invalid rows removed")

    batch_size = 10
    batches = [df.iloc[i:i+batch_size] for i in range(0, len(df), batch_size)]
    with pd.ExcelWriter('final_verified.xlsx') as writer:
        for idx, batch in enumerate(batches, 1):
            batch.to_excel(writer, sheet_name=f'Batch {idx}', index=False)
    df.to_csv('final_verified.csv', index=False)
    st.info("✅ Step 5: Batching done")

    # Step 6: Filter by email domain
    website_col = next((col for col in df.columns if 'website' in col.lower()), None)
    before_filter_count = len(df)
    df = filter_leads_by_domain(df, email_col, website_col)
    after_filter_count = len(df)
    st.info(f"✅ Step 6: Domain filtering done (kept {after_filter_count} out of {before_filter_count} leads)")

    # Step 7: Filter by excluded names (optional)
    name_filter_removed = 0
    if excluded_names and len(excluded_names) > 0:
        name_col = next((col for col in df.columns if 'name' in col.lower()), None)
        if name_col:
            before_name_filter_count = len(df)
            df = filter_leads_by_excluded_names(df, excluded_names, name_col)
            after_name_filter_count = len(df)
            name_filter_removed = before_name_filter_count - after_name_filter_count
            st.info(f"✅ Step 7: Name filtering done (removed {name_filter_removed} leads with excluded names, kept {after_name_filter_count} leads)")
        else:
            st.warning("⚠️ No 'Name' column found. Skipping name filter.")

    # Save filtered results
    final_count = len(df)
    batch_size = 10
    batches = [df.iloc[i:i+batch_size] for i in range(0, len(df), batch_size)]
    with pd.ExcelWriter('final_verified.xlsx') as writer:
        for idx, batch in enumerate(batches, 1):
            batch.to_excel(writer, sheet_name=f'Batch {idx}', index=False)
    df.to_csv('final_verified.csv', index=False)

    # Build summary message
    summary_msg = (
        f"🎉 All Done!\n\n"
        f"✅ Valid: {valid_count}\n"
        f"⚠️ Risky: {risky_count}\n"
        f"❌ Invalid: {invalid_count} (removed)\n"
        f"📧 After domain filter: {after_filter_count} leads"
    )
    if name_filter_removed > 0:
        summary_msg += f"\n🚫 Name filter removed: {name_filter_removed} leads"
    summary_msg += f"\n\n📊 Final leads: {final_count}"
    
    final_summary_area.success(summary_msg)

    return "final_verified.xlsx"

# --------------------------
# ✅ STREAMLIT UI
# --------------------------
st.title("📧 Perfect Bulk Email Verifier — Live Table View")

uploaded_file = st.file_uploader(
    "Upload your CSV file with an 'Email' column.",
    type=['csv']
)

# Optional: Excluded names field
st.subheader("🔧 Optional Filters")
excluded_names_text = st.text_area(
    "Exclude Leads by Name (Optional)",
    placeholder="Enter names to exclude, one per line or separated by commas.\nExample:\nJohn Doe\nJane Smith\nABC Company",
    help="Enter names of leads you want to exclude. Each name should be on a new line or separated by commas. Matching is case-insensitive."
)

if uploaded_file:
    if st.button("Start Verification"):
        progress_area = st.progress(0)
        table_area = st.empty()
        final_summary_area = st.empty()

        st.info("⏳ Running... Watch the live verification table below:")

        # Parse excluded names
        excluded_names = None
        if excluded_names_text and excluded_names_text.strip():
            # Split by newlines and commas, then clean up
            names_list = []
            for line in excluded_names_text.split('\n'):
                # Also split by comma in case user uses comma-separated format
                for name in line.split(','):
                    name = name.strip()
                    if name:
                        names_list.append(name)
            excluded_names = names_list if names_list else None

        try:
            output_path = process_file(uploaded_file, progress_area, table_area, final_summary_area, excluded_names)

            with open(output_path, "rb") as f:
                st.download_button("📄 Download Excel (Batches)", f, file_name="final_verified.xlsx")

            with open('final_verified.csv', "rb") as f:
                st.download_button("📄 Download CSV (Flat)", f, file_name="final_verified.csv")

        except Exception as e:
            st.error(f"❌ Error: {e}")
