# create_env.py - Run this once to create your .env file
import os

# ⚠️ REPLACE with your actual keys
PUBLIC_KEY="pk_live_HMppfW57pws-9IW7IBSBB7i6"
SECRET_KEY="sk_live_mLSe1ozbIEihHh8rqTA3TNCONEWjt_sPukxU_koINcQcQCnq"

with open('.env', 'w') as f:
    f.write(f"BAYSE_PUBLIC_KEY={PUBLIC_KEY}\n")
    f.write(f"BAYSE_SECRET_KEY={SECRET_KEY}\n")

print(".env file created successfully!")
print(f"Location: {os.path.abspath('.env')}")


