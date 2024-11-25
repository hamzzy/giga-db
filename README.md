# **GPU-Accelerated S3-Based Columnar Database**

A high-performance columnar database, leveraging **GPU acceleration** for query execution and for object storage.

---

## **Features**
- **S3 Integration**: Stream data directly from Amazon S3 for scalable storage.
- **Columnar Data Processing**: Optimized for analytical queries with efficient columnar storage.
- **GPU Acceleration**: Use the power of GPU for lightning-fast data processing.
- **SQL-like Query Support**: Execute basic SQL queries (`SELECT`, `WHERE`, `AGGREGATE`) on large datasets.

---

## **Getting Started**

### **Prerequisites**
1. **Rust**: Install Rust and Cargo from [rust-lang.org](https://www.rust-lang.org/tools/install).
2. **Amazon S3**: 
   - An AWS account with access to an S3 bucket.
   - AWS credentials configured locally (`~/.aws/credentials` or environment variables).
3. **GPU Support**:
   - macOS: Metal-based acceleration.
   - Linux/Windows: Ensure NVIDIA GPU drivers and CUDA toolkit are installed if using CUDA.

### **Setup**
1. Clone the repository:
   ```bash
   git clone https://github.com/your-repo-name/gpu_columnar_db.git
   cd gpu_columnar_db
