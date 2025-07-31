# Data Streaming Framework - Documentation Index

This index provides quick access to all framework documentation.

## 📚 Core Documentation

### [FRAMEWORK_README.md](FRAMEWORK_README.md)
**Main framework documentation** - Start here for overview, quick start, and comprehensive configuration guide.

**Contents:**
- 🚀 Quick Start Guide
- 📋 Framework Features & Architecture  
- ⚙️ Configuration Examples
- 🔄 **MQ Message Parsing** (NEW)
- 🔧 Contract Management
- 📊 Monitoring & Observability
- 🔐 Security Configuration
- 🚀 Deployment Guides
- 🎯 Performance Tuning
- 🧪 Testing Strategies

---

## 🔄 MQ Message Parsing

### [MQ_PARSING_USER_GUIDE.md](MQ_PARSING_USER_GUIDE.md) ⭐ **NEW**
**Comprehensive guide for MQ message parsing capabilities**

**Contents:**
- 📋 Overview & Quick Start
- 📨 Message Formats (Fixed-Length, Delimited, Custom)
- 🔧 Contract Definition & Configuration
- 💻 Programming Interface & APIs
- 🎯 Best Practices & Performance Tips
- 🐛 Troubleshooting & Common Issues
- 📚 Complete Examples & Use Cases

**Key Features Covered:**
- ✅ Fixed-length message parsing with position-based fields
- ✅ CSV, TSV, pipe-delimited message support  
- ✅ JSON-based externalized contracts
- ✅ Type-safe processing with automatic conversion
- ✅ Comprehensive validation & error handling
- ✅ Spring Boot integration & auto-configuration

---

## 🌐 Distributed Operations

### [DISTRIBUTED_TRACKING_README.md](DISTRIBUTED_TRACKING_README.md)
**Multi-instance deployment and distributed tracking**

**Contents:**
- 🚀 Distributed Message Tracking
- 📊 Aggregate Metrics Collection
- 💚 Multi-Instance Health Monitoring
- ⚙️ Redis Configuration
- 🔧 Cluster Management
- 📈 Performance Metrics

---

## 🏗️ Build & Deployment

### [BUILD_RESULTS.md](BUILD_RESULTS.md)
**Build verification and compilation status**

### [ENTERPRISE_DEPLOYMENT_GUIDE.md](ENTERPRISE_DEPLOYMENT_GUIDE.md)  
**Enterprise deployment patterns and best practices**

---

## 📊 Status Reports

### [MQ_PARSING_SUCCESS_REPORT.md](MQ_PARSING_SUCCESS_REPORT.md) ⭐ **NEW**
**Implementation verification and testing results for MQ parsing**

**Contents:**
- ✅ Implementation Status & Verification
- 🔧 Component Testing Results
- 🚀 Application Startup Verification
- 📋 Sample Contracts & Examples
- ⚙️ Configuration Integration
- 🏆 Success Criteria Confirmation

---

## 🎯 Quick Reference

### 🚀 **Getting Started**
1. **New Users**: Start with [FRAMEWORK_README.md](FRAMEWORK_README.md) → Quick Start Guide
2. **MQ Parsing**: Read [MQ_PARSING_USER_GUIDE.md](MQ_PARSING_USER_GUIDE.md) for message parsing
3. **Multi-Instance**: Check [DISTRIBUTED_TRACKING_README.md](DISTRIBUTED_TRACKING_README.md) for clustering

### 🔍 **Find Information By Topic**

| Topic | Primary Document | Section |
|-------|------------------|---------|
| **Framework Overview** | [FRAMEWORK_README.md](FRAMEWORK_README.md) | Overview & Features |
| **Quick Setup** | [FRAMEWORK_README.md](FRAMEWORK_README.md) | Quick Start Guide |
| **MQ Message Parsing** | [MQ_PARSING_USER_GUIDE.md](MQ_PARSING_USER_GUIDE.md) | Complete Guide |
| **Configuration** | [FRAMEWORK_README.md](FRAMEWORK_README.md) | Configuration Guide |
| **Contract Management** | [FRAMEWORK_README.md](FRAMEWORK_README.md) | Contract Management |
| **MQ Contracts** | [MQ_PARSING_USER_GUIDE.md](MQ_PARSING_USER_GUIDE.md) | Contract Definition |
| **Multi-Instance Setup** | [DISTRIBUTED_TRACKING_README.md](DISTRIBUTED_TRACKING_README.md) | Distributed Operations |
| **Monitoring** | [FRAMEWORK_README.md](FRAMEWORK_README.md) | Monitoring & Observability |
| **Security** | [FRAMEWORK_README.md](FRAMEWORK_README.md) | Security Configuration |
| **Performance** | [FRAMEWORK_README.md](FRAMEWORK_README.md) | Performance Tuning |
| **Troubleshooting** | [MQ_PARSING_USER_GUIDE.md](MQ_PARSING_USER_GUIDE.md) | Troubleshooting |
| **Examples** | [MQ_PARSING_USER_GUIDE.md](MQ_PARSING_USER_GUIDE.md) | Examples |

### 🔄 **Recent Updates**

#### ⭐ **Latest: MQ Message Parsing (v1.0.0)**
- **New Feature**: Comprehensive MQ message parsing with externalized contracts
- **Formats Supported**: Fixed-length, CSV, TSV, pipe-delimited, custom
- **Type Safety**: Automatic conversion to Java types with validation
- **Documentation**: Complete user guide with examples and best practices
- **Integration**: Seamless Spring Boot auto-configuration

#### 🌐 **Distributed Tracking**
- **Multi-Instance Support**: Redis-based distributed tracking
- **Cluster Monitoring**: Aggregate metrics and health monitoring
- **High Availability**: Cross-instance message reconciliation

#### 🏗️ **Enterprise Features**
- **Security**: SSL/TLS, SASL authentication
- **Monitoring**: Prometheus metrics, health checks
- **Performance**: High-throughput optimization (1000+ TPS)
- **Deployment**: Docker, Kubernetes support

---

## 📧 Support & Contributing

- **Issues**: Report bugs and request features via GitHub Issues
- **Documentation**: This documentation index for quick navigation
- **Enterprise Support**: Contact your framework team for advanced support
- **Contributing**: See individual documents for extension points and customization guides

---

## 🗂️ File Structure

```
datastreaming-framework/
├── DOCUMENTATION_INDEX.md           # This index (start here)
├── FRAMEWORK_README.md               # Main framework documentation  
├── MQ_PARSING_USER_GUIDE.md         # MQ parsing comprehensive guide
├── MQ_PARSING_SUCCESS_REPORT.md     # Implementation verification report
├── DISTRIBUTED_TRACKING_README.md   # Multi-instance operations
├── BUILD_RESULTS.md                 # Build and testing status
├── ENTERPRISE_DEPLOYMENT_GUIDE.md   # Enterprise deployment patterns
├── ERROR_HANDLING_AND_ALERTING_GUIDE.md # Comprehensive error handling guide
├── TEAMS_INTEGRATION_GUIDE.md       # Microsoft Teams notification setup
└── README.md                        # Project overview
```

**Happy streaming! 🚀**