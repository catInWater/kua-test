#!/bin/bash
# gen_rand_files.sh
# 快速生成不同大小的随机文件用于实验

# 生成函数
gen_file() {
    local size=$1
    local file=$2
    echo "Generating $file ($size)..."
    head -c "$size" /dev/urandom > "$file"
    echo "$file created with size $(ls -lh $file | awk '{print $5}')"
}

# 生成文件
gen_file 10240 /tmp/rand10k       # 10 KB
gen_file 102400 /tmp/rand100k     # 100 KB
gen_file 1048576 /tmp/rand1m      # 1 MB
gen_file 5242880 /tmp/rand5m      # 5 MB

echo "All files generated."