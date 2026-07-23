#!/bin/bash
set -e

# 颜色输出定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # 重置颜色

echo -e "${BLUE}==================================================${NC}"
echo -e "${BLUE}    gcli2api Debian 一键部署与守护服务配置脚本    ${NC}"
echo -e "${BLUE}==================================================${NC}"

# 获取脚本所在的项目根目录
INSTALL_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SERVICE_NAME="gcli2api"
SERVICE_FILE="/etc/systemd/system/${SERVICE_NAME}.service"

# 权限检测
if [ "$EUID" -ne 0 ]; then
    echo -e "${RED}[错误] 请使用 root 权限或 sudo 运行此脚本！${NC}"
    exit 1
fi

# ==================== [步骤 1/4] 配置代理端口 ====================
echo -e "\n${YELLOW}[步骤 1/4] 配置网络代理 (PROXY)...${NC}"
DEFAULT_PROXY="http://127.0.0.1:48898"
PROXY_URL=""

# 支持从命令行参数获取代理，例如: ./debian-deploy.sh http://127.0.0.1:7890
if [ -n "${1:-}" ]; then
    PROXY_URL="$1"
    echo -e "${GREEN}[+] 从脚本参数读取代理地址: ${PROXY_URL}${NC}"
else
    # 交互式询问用户，直接回车使用默认的预设端口
    read -p "请输入上游代理地址 (默认预设: ${DEFAULT_PROXY}，如无需代理请输入 none): " input_proxy
    if [ -z "$input_proxy" ]; then
        PROXY_URL="$DEFAULT_PROXY"
    elif [ "$input_proxy" = "none" ] || [ "$input_proxy" = "NONE" ]; then
        PROXY_URL=""
    else
        PROXY_URL="$input_proxy"
    fi
fi

if [ -n "$PROXY_URL" ]; then
    echo -e "${GREEN}[+] 代理生效地址: ${PROXY_URL}${NC}"
else
    echo -e "${BLUE}[i] 未配置代理，服务将直连访问。${NC}"
fi

# ==================== [步骤 2/4] 检测与安装依赖环境 ====================
echo -e "\n${YELLOW}[步骤 2/4] 检测项目环境...${NC}"
echo -e "${BLUE}[i] 项目根目录定位为: ${INSTALL_DIR}${NC}"

cd "$INSTALL_DIR"

if [ -d "$INSTALL_DIR/.venv" ] && [ -f "$INSTALL_DIR/.venv/bin/python" ]; then
    echo -e "${GREEN}[+] 检测到 Python 虚拟环境已建立，跳过环境依赖安装。${NC}"
else
    echo -e "${BLUE}[i] 开始安装系统基础依赖 (python3, python3-venv, python3-pip)...${NC}"
    apt-get update -y
    apt-get install -y python3 python3-venv python3-pip curl

    echo -e "${BLUE}[i] 创建 Python 虚拟环境 (.venv)...${NC}"
    python3 -m venv .venv

    echo -e "${BLUE}[i] 安装 Python 项目依赖包...${NC}"
    .venv/bin/pip install --upgrade pip
    .venv/bin/pip install -r requirements.txt

    echo -e "${GREEN}[+] 项目环境依赖安装完成！${NC}"
fi

# ==================== [步骤 3/4] 检查与设置 Systemd 服务 ====================
echo -e "\n${YELLOW}[步骤 3/4] 检查与配置 Systemd 后台守护服务...${NC}"

# 如果配置了代理，写入 Systemd 环境变量
ENV_LINE=""
if [ -n "$PROXY_URL" ]; then
    ENV_LINE="Environment=\"PROXY=${PROXY_URL}\""
fi

# 写入或覆盖 Service 配置文件
cat <<EOF > "$SERVICE_FILE"
[Unit]
Description=gcli2api OpenAI Proxy Service
After=network.target

[Service]
Type=simple
User=root
WorkingDirectory=${INSTALL_DIR}
ExecStart=${INSTALL_DIR}/.venv/bin/python web.py
${ENV_LINE}
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF

echo -e "${GREEN}[+] Systemd 配置文件更新完成: ${SERVICE_FILE}${NC}"

# 重新加载 systemd 并设置开机自启
systemctl daemon-reload
systemctl enable "$SERVICE_NAME" > /dev/null 2>&1
echo -e "${GREEN}[+] 已成功设置为开机自动启动服务。${NC}"

# ==================== [步骤 4/4] 启动服务与运行检查 ====================
echo -e "\n${YELLOW}[步骤 4/4] 启动服务并检查运行状态...${NC}"

systemctl restart "$SERVICE_NAME"
sleep 2

if systemctl is-active --quiet "$SERVICE_NAME"; then
    echo -e "\n${GREEN}==================================================${NC}"
    echo -e "${GREEN}   ✔ gcli2api 服务已就绪并在后台稳定运行！   ${NC}"
    echo -e "${GREEN}==================================================${NC}"
    echo -e "项目路径：${BLUE}${INSTALL_DIR}${NC}"
    echo -e "服务端口：${BLUE}7861${NC}"
    echo -e "常用管理命令："
    echo -e "  查看运行状态:  ${BLUE}systemctl status ${SERVICE_NAME}${NC}"
    echo -e "  查看实时日志:  ${BLUE}journalctl -u ${SERVICE_NAME} -f${NC}"
    echo -e "  重启后台服务:  ${BLUE}systemctl restart ${SERVICE_NAME}${NC}"
    echo -e "  停止后台服务:  ${BLUE}systemctl stop ${SERVICE_NAME}${NC}"
else
    echo -e "${RED}[!] 服务启动异常，请运行以下命令查看日志：${NC}"
    echo -e "  ${YELLOW}journalctl -u ${SERVICE_NAME} -n 30${NC}"
    exit 1
fi
