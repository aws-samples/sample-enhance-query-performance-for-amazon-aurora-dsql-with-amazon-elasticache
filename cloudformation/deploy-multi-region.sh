#!/bin/bash

# Simple Multi-Region DSQL Deployment Script
# Deploys DSQL clusters to us-east-1, us-west-2, and witness in us-east-2

set -e

# Configuration
STACK_NAME="dsql-multi-region"
ENVIRONMENT="dev"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Display usage
usage() {
    echo "Usage: $0 [phase]"
    echo
    echo "Phases:"
    echo "  initial  - Deploy initial clusters (Phase 1)"
    echo "  link     - Link clusters for multi-region setup (Phase 2)"
    echo "  status   - Check deployment status"
    echo "  cleanup  - Delete all stacks"
    echo
    echo "Example:"
    echo "  $0 initial  # Deploy all initial clusters"
    echo "  $0 link     # Link clusters together"
}

# Check prerequisites
check_prerequisites() {
    log_info "Checking prerequisites..."
    
    if ! command -v aws &> /dev/null; then
        log_error "AWS CLI is not installed"
        exit 1
    fi
    
    if ! aws sts get-caller-identity &> /dev/null; then
        log_error "AWS credentials not configured"
        exit 1
    fi
    
    log_success "Prerequisites check passed"
}

# Deploy Phase 1 - Initial clusters
deploy_initial() {
    log_info "PHASE 1: Deploying initial clusters..."
    
    # Deploy us-east-1 (Cluster with ElastiCache)
    log_info "Deploying us-east-1 cluster (DSQL + ElastiCache)..."
    aws cloudformation create-stack \
        --stack-name "$STACK_NAME" \
        --template-body "file://dsql-multi-region-us-east-1.yaml" \
        --region "us-east-1" \
        --parameters "ParameterKey=Environment,ParameterValue=$ENVIRONMENT" \
        --tags "Key=Environment,Value=$ENVIRONMENT" "Key=Purpose,Value=DSQL-Multi-Region"
    
    # Deploy us-west-2 (Cluster with ElastiCache)
    log_info "Deploying us-west-2 cluster (DSQL + ElastiCache)..."
    aws cloudformation create-stack \
        --stack-name "$STACK_NAME" \
        --template-body "file://dsql-multi-region-us-west-2.yaml" \
        --region "us-west-2" \
        --parameters "ParameterKey=Environment,ParameterValue=$ENVIRONMENT" \
        --tags "Key=Environment,Value=$ENVIRONMENT" "Key=Purpose,Value=DSQL-Multi-Region"
    
    # Deploy us-east-2 (Witness)
    log_info "Deploying us-east-2 witness..."
    aws cloudformation create-stack \
        --stack-name "$STACK_NAME-witness" \
        --template-body "file://dsql-multi-region-us-east-2-witness.yaml" \
        --region "us-east-2" \
        --parameters "ParameterKey=Environment,ParameterValue=$ENVIRONMENT" \
        --tags "Key=Environment,Value=$ENVIRONMENT" "Key=Purpose,Value=DSQL-Multi-Region-Witness"
    
    log_success "Initial deployment initiated in all regions"
    log_info "Run '$0 status' to check deployment progress"
    log_warning "After all stacks are CREATE_COMPLETE, run '$0 link' to connect them"
}

# Get cluster ARNs
get_cluster_arns() {
    local east_arn=$(aws cloudformation describe-stacks \
        --stack-name "$STACK_NAME" \
        --region "us-east-1" \
        --query 'Stacks[0].Outputs[?OutputKey==`DSQLClusterArn`].OutputValue' \
        --output text 2>/dev/null || echo "")
    
    local west_arn=$(aws cloudformation describe-stacks \
        --stack-name "$STACK_NAME" \
        --region "us-west-2" \
        --query 'Stacks[0].Outputs[?OutputKey==`DSQLClusterArn`].OutputValue' \
        --output text 2>/dev/null || echo "")
    
    echo "$east_arn,$west_arn"
}

# Deploy Phase 2 - Link clusters
link_clusters() {
    log_info "PHASE 2: Linking clusters for multi-region setup..."
    
    # Get cluster ARNs
    IFS=',' read -r EAST_ARN WEST_ARN <<< "$(get_cluster_arns)"
    
    if [[ -z "$EAST_ARN" || -z "$WEST_ARN" ]]; then
        log_error "Could not retrieve cluster ARNs. Ensure Phase 1 is complete."
        log_info "Run '$0 status' to check current deployment status"
        exit 1
    fi
    
    log_info "East cluster ARN: $EAST_ARN"
    log_info "West cluster ARN: $WEST_ARN"
    
    # Update us-east-1 stack
    log_info "Updating us-east-1 stack for linking..."
    aws cloudformation update-stack \
        --stack-name "$STACK_NAME" \
        --template-body "file://dsql-multi-region-us-east-1.yaml" \
        --region "us-east-1" \
        --parameters \
            "ParameterKey=Environment,ParameterValue=$ENVIRONMENT" \
            "ParameterKey=DeploymentPhase,ParameterValue=Linking" \
            "ParameterKey=WestClusterArn,ParameterValue=$WEST_ARN"
    
    # Update us-west-2 stack
    log_info "Updating us-west-2 stack for linking..."
    aws cloudformation update-stack \
        --stack-name "$STACK_NAME" \
        --template-body "file://dsql-multi-region-us-west-2.yaml" \
        --region "us-west-2" \
        --parameters \
            "ParameterKey=Environment,ParameterValue=$ENVIRONMENT" \
            "ParameterKey=DeploymentPhase,ParameterValue=Linking" \
            "ParameterKey=EastClusterArn,ParameterValue=$EAST_ARN"
    
    log_success "Linking updates initiated"
    log_info "Run '$0 status' to check linking progress"
}

# Check deployment status
check_status() {
    log_info "Checking deployment status..."
    
    echo
    echo "=== DEPLOYMENT STATUS ==="
    
    # Check us-east-1
    local east_status=$(aws cloudformation describe-stacks \
        --stack-name "$STACK_NAME" \
        --region "us-east-1" \
        --query 'Stacks[0].StackStatus' \
        --output text 2>/dev/null || echo "NOT_DEPLOYED")
    echo "us-east-1 (DSQL + ElastiCache): $east_status"
    
    # Check us-west-2
    local west_status=$(aws cloudformation describe-stacks \
        --stack-name "$STACK_NAME" \
        --region "us-west-2" \
        --query 'Stacks[0].StackStatus' \
        --output text 2>/dev/null || echo "NOT_DEPLOYED")
    echo "us-west-2 (DSQL + ElastiCache): $west_status"
    
    # Check us-east-2 witness
    local witness_status=$(aws cloudformation describe-stacks \
        --stack-name "$STACK_NAME-witness" \
        --region "us-east-2" \
        --query 'Stacks[0].StackStatus' \
        --output text 2>/dev/null || echo "NOT_DEPLOYED")
    echo "us-east-2 (Witness):               $witness_status"
    
    echo
    
    # Show endpoints if clusters are ready
    if [[ "$east_status" == "CREATE_COMPLETE" || "$east_status" == "UPDATE_COMPLETE" ]]; then
        local east_endpoint=$(aws cloudformation describe-stacks \
            --stack-name "$STACK_NAME" \
            --region "us-east-1" \
            --query 'Stacks[0].Outputs[?OutputKey==`DSQLClusterEndpoint`].OutputValue' \
            --output text 2>/dev/null)
        echo "us-east-1 DSQL Endpoint: $east_endpoint"
    fi
    
    if [[ "$west_status" == "CREATE_COMPLETE" || "$west_status" == "UPDATE_COMPLETE" ]]; then
        local west_endpoint=$(aws cloudformation describe-stacks \
            --stack-name "$STACK_NAME" \
            --region "us-west-2" \
            --query 'Stacks[0].Outputs[?OutputKey==`DSQLClusterEndpoint`].OutputValue' \
            --output text 2>/dev/null)
        echo "us-west-2 DSQL Endpoint: $west_endpoint"
    fi
    
    # Show ElastiCache endpoints
    if [[ "$east_status" == "CREATE_COMPLETE" || "$east_status" == "UPDATE_COMPLETE" ]]; then
        local valkey_endpoint_east=$(aws cloudformation describe-stacks \
            --stack-name "$STACK_NAME" \
            --region "us-east-1" \
            --query 'Stacks[0].Outputs[?OutputKey==`ValkeyFullEndpoint`].OutputValue' \
            --output text 2>/dev/null)
        echo "us-east-1 Valkey Endpoint: $valkey_endpoint_east"
    fi
    
    if [[ "$west_status" == "CREATE_COMPLETE" || "$west_status" == "UPDATE_COMPLETE" ]]; then
        local valkey_endpoint_west=$(aws cloudformation describe-stacks \
            --stack-name "$STACK_NAME" \
            --region "us-west-2" \
            --query 'Stacks[0].Outputs[?OutputKey==`ValkeyFullEndpoint`].OutputValue' \
            --output text 2>/dev/null)
        echo "us-west-2 Valkey Endpoint: $valkey_endpoint_west"
    fi
}

# Cleanup all stacks
cleanup() {
    log_warning "This will delete ALL stacks. Are you sure? (y/N)"
    read -r confirmation
    if [[ $confirmation != [yY] ]]; then
        log_info "Cleanup cancelled"
        exit 0
    fi
    
    log_info "Deleting all stacks..."
    
    # Delete stacks in parallel
    aws cloudformation delete-stack --stack-name "$STACK_NAME" --region "us-east-1" &
    aws cloudformation delete-stack --stack-name "$STACK_NAME" --region "us-west-2" &
    aws cloudformation delete-stack --stack-name "$STACK_NAME-witness" --region "us-east-2" &
    
    wait
    log_success "Cleanup initiated in all regions"
}

# Main execution
case "${1:-}" in
    initial)
        check_prerequisites
        deploy_initial
        ;;
    link)
        check_prerequisites
        link_clusters
        ;;
    status)
        check_status
        ;;
    cleanup)
        cleanup
        ;;
    *)
        usage
        exit 1
        ;;
esac
