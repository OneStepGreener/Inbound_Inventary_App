# Network/DNS Resolution Issue - Fix Instructions

## Problem
Gradle build is failing because it cannot resolve Maven repository hostnames (dl.google.com, repo.maven.apache.org, etc.). DNS queries are timing out.

## Solutions (Try in order)

### Solution 1: Check Internet Connection
1. Open a web browser and verify you can access websites
2. If you cannot browse the internet, check your network connection first

### Solution 2: Change DNS Servers
Your system is using 8.8.8.8 (Google DNS) but queries are timing out. Try:

**Option A: Use Cloudflare DNS (Recommended)**
1. Open Network Settings:
   - Press `Win + I` → Network & Internet → Change adapter options
   - Right-click your active network adapter → Properties
   - Select "Internet Protocol Version 4 (TCP/IPv4)" → Properties
   - Select "Use the following DNS server addresses"
   - Preferred: `1.1.1.1`
   - Alternate: `1.0.0.1`
   - Click OK

**Option B: Use Automatic DNS**
- Select "Obtain DNS server address automatically" in the same settings

### Solution 3: Check Firewall/Antivirus
- Temporarily disable Windows Firewall or Antivirus
- Try the build again
- If it works, add exceptions for Gradle/Java

### Solution 4: Check Corporate Proxy/VPN
If you're on a corporate network:
- Check if a proxy is required
- Configure proxy in `android/gradle.properties`:
  ```
  systemProp.http.proxyHost=proxy.company.com
  systemProp.http.proxyPort=8080
  systemProp.https.proxyHost=proxy.company.com
  systemProp.https.proxyPort=8080
  ```

### Solution 5: Restart Network Adapter
Run in PowerShell as Administrator:
```powershell
ipconfig /release
ipconfig /renew
ipconfig /flushdns
netsh winsock reset
```

Then restart your computer.

### Solution 6: Check Network Adapter Status
```powershell
Get-NetAdapter | Where-Object Status -eq "Up"
```

### Solution 7: Test DNS Resolution
After changing DNS, test with:
```powershell
nslookup repo.maven.apache.org
ping dl.google.com
```

## After Fixing DNS
Once DNS is working, try the build again:
```bash
cd android
./gradlew clean
cd ..
npx react-native run-android
```

