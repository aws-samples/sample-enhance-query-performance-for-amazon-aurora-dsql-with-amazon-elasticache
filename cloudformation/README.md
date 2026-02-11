# Multi-Region DSQL + ElastiCache Setup

This directory contains CloudFormation templates and deployment scripts for setting up a multi-region DSQL cluster with ElastiCache integration.

## Architecture Diagram

<div align="center">
  <img src="dsql-architecture-diagram.png" alt="Multi-Region DSQL + ElastiCache Architecture" width="800">
</div>

## Architecture Overview

- **us-east-1**: DSQL cluster + Full VPC + ElastiCache (Valkey)
- **us-west-2**: DSQL cluster + Full VPC + ElastiCache (Valkey)
- **us-east-2**: Witness region for DSQL (minimal infrastructure)

## Files

- `dsql-multi-region-us-east-1.yaml` - us-east-1 cluster with ElastiCache
- `dsql-multi-region-us-west-2.yaml` - us-west-2 cluster with ElastiCache
- `dsql-multi-region-us-east-2-witness.yaml` - Witness region template
- `deploy-multi-region.sh` - Deployment script
- `dsql-architecture-diagram.png` - Architecture diagram

## Quick Start

### Option 1: Using the Deployment Script (Recommended)

```bash
# Phase 1: Deploy initial clusters
./deploy-multi-region.sh initial

# Check status
./deploy-multi-region.sh status

# Phase 2: Link clusters (after all are CREATE_COMPLETE)
./deploy-multi-region.sh link

# Check final status
./deploy-multi-region.sh status
```

### Option 2: Manual Deployment

#### Phase 1: Deploy Initial Clusters

```bash
# Deploy us-east-1 (DSQL + ElastiCache)
aws cloudformation create-stack \
  --stack-name dsql-multi-region \
  --template-body file://dsql-multi-region-us-east-1.yaml \
  --region us-east-1 \
  --parameters ParameterKey=Environment,ParameterValue=dev

# Deploy us-west-2 (DSQL + ElastiCache)
aws cloudformation create-stack \
  --stack-name dsql-multi-region \
  --template-body file://dsql-multi-region-us-west-2.yaml \
  --region us-west-2 \
  --parameters ParameterKey=Environment,ParameterValue=dev

# Deploy us-east-2 (Witness)
aws cloudformation create-stack \
  --stack-name dsql-multi-region-witness \
  --template-body file://dsql-multi-region-us-east-2-witness.yaml \
  --region us-east-2 \
  --parameters ParameterKey=Environment,ParameterValue=dev
```

#### Phase 2: Get Cluster ARNs

```bash
# Get us-east-1 cluster ARN
EAST_ARN=$(aws cloudformation describe-stacks \
  --stack-name dsql-multi-region \
  --region us-east-1 \
  --query 'Stacks[0].Outputs[?OutputKey==`DSQLClusterArn`].OutputValue' \
  --output text)

# Get us-west-2 cluster ARN  
WEST_ARN=$(aws cloudformation describe-stacks \
  --stack-name dsql-multi-region \
  --region us-west-2 \
  --query 'Stacks[0].Outputs[?OutputKey==`DSQLClusterArn`].OutputValue' \
  --output text)

echo "East ARN: $EAST_ARN"
echo "West ARN: $WEST_ARN"
```

#### Phase 3: Link Clusters

```bash
# Update us-east-1 stack for linking
aws cloudformation update-stack \
  --stack-name dsql-multi-region \
  --template-body file://dsql-multi-region-us-east-1.yaml \
  --region us-east-1 \
  --parameters \
    ParameterKey=Environment,ParameterValue=dev \
    ParameterKey=DeploymentPhase,ParameterValue=Linking \
    ParameterKey=WestClusterArn,ParameterValue=$WEST_ARN

# Update us-west-2 stack for linking
aws cloudformation update-stack \
  --stack-name dsql-multi-region \
  --template-body file://dsql-multi-region-us-west-2.yaml \
  --region us-west-2 \
  --parameters \
    ParameterKey=Environment,ParameterValue=dev \
    ParameterKey=DeploymentPhase,ParameterValue=Linking \
    ParameterKey=EastClusterArn,ParameterValue=$EAST_ARN
```

## Getting Connection Information

After successful deployment:

```bash
# DSQL Endpoints
aws cloudformation describe-stacks \
  --stack-name dsql-multi-region \
  --region us-east-1 \
  --query 'Stacks[0].Outputs[?OutputKey==`DSQLClusterEndpoint`].OutputValue' \
  --output text

aws cloudformation describe-stacks \
  --stack-name dsql-multi-region \
  --region us-west-2 \
  --query 'Stacks[0].Outputs[?OutputKey==`DSQLClusterEndpoint`].OutputValue' \
  --output text

# ElastiCache Endpoints
# us-east-1 ElastiCache
aws cloudformation describe-stacks \
  --stack-name dsql-multi-region \
  --region us-east-1 \
  --query 'Stacks[0].Outputs[?OutputKey==`ValkeyFullEndpoint`].OutputValue' \
  --output text

# us-west-2 ElastiCache
aws cloudformation describe-stacks \
  --stack-name dsql-multi-region \
  --region us-west-2 \
  --query 'Stacks[0].Outputs[?OutputKey==`ValkeyFullEndpoint`].OutputValue' \
  --output text
```

## CloudShell VPC Access

If you want to access these resources from CloudShell:

### For us-east-1:
```bash
# Get VPC and subnet information
aws cloudformation describe-stacks \
  --stack-name dsql-multi-region \
  --region us-east-1 \
  --query 'Stacks[0].Outputs[?OutputKey==`VPCId`].OutputValue' \
  --output text
```

Then configure CloudShell VPC access in the AWS Console:
- Go to CloudShell → Settings → VPC
- Use the VPC ID and private subnets from the stack outputs

### For us-west-2:
Similar process using us-west-2 outputs.

## Network Configuration

- **us-east-1 VPC**: 10.0.0.0/16
- **us-west-2 VPC**: 10.1.0.0/16
- All VPCs have public/private subnets with NAT Gateways
- ElastiCache exists in both regions for symmetric architecture

## Cleanup

```bash
# Using script
./deploy-multi-region.sh cleanup

# Or manually
aws cloudformation delete-stack --stack-name dsql-multi-region --region us-east-1
aws cloudformation delete-stack --stack-name dsql-multi-region --region us-west-2
aws cloudformation delete-stack --stack-name dsql-multi-region-witness --region us-east-2
```

## Key Differences from Single-Region

1. **Multi-region DSQL**: Clusters in us-east-1 and us-west-2 are linked with witness in us-east-2
2. **Symmetric architecture**: Both regions have identical infrastructure (DSQL + ElastiCache + VPC)
3. **Network isolation**: Each region has its own VPC with different CIDR ranges
4. **Deployment phases**: Two-phase deployment (initial + linking) required for multi-region setup

## Troubleshooting

- **Phase 1 fails**: Check AWS region availability and service limits
- **Phase 2 fails**: Ensure both clusters are in CREATE_COMPLETE status
- **Can't access resources**: Verify VPC configuration and security groups
- **ElastiCache performance**: Each region has its own ElastiCache for optimal performance

## Costs

This setup provisions:
- 2 full VPC infrastructures (NAT Gateways, etc.)
- 2 DSQL clusters
- 2 ElastiCache serverless clusters (one per region)
- Standard AWS service costs apply

Each region is fully self-contained with local ElastiCache for optimal performance.
