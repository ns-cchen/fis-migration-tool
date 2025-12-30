// Copyright (c) 2024 Netskope, Inc. All rights reserved.

package s3

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/netSkope/fis-migration-tool/internal/config"
	"github.com/netSkope/fis-migration-tool/internal/util"
	"go.uber.org/zap"
)

const (
	// Multipart upload threshold: 5MB
	multipartThreshold = 5 * 1024 * 1024
	// Max retries for S3 operations
	maxS3Retries = 5
	// Initial retry delay
	initialRetryDelay = 1 * time.Second
)

// Uploader handles S3 uploads with multipart support.
type Uploader struct {
	s3Client   *s3.S3
	uploader   *s3manager.Uploader
	config     *config.Config
	logger     *zap.Logger
}

// NewUploader creates a new S3 uploader.
func NewUploader(cfg *config.Config, logger *zap.Logger) (*Uploader, error) {
	// Try to load AWS credentials from Vault (for Kubernetes environments)
	// This will be a no-op if Vault is not available, and the SDK will fall back
	// to the default credential chain (~/.aws/credentials, environment variables, etc.)
	util.LoadAWSCredentialsFromVault()

	// Create AWS session with optional custom endpoint (for LocalStack testing)
	// The session will use the default credential chain which includes:
	// 1. Environment variables (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
	// 2. Shared credentials file (~/.aws/credentials)
	// 3. IAM role (if running on EC2)
	// 4. Vault credentials (if LoadAWSCredentialsFromVault set them)
	awsConfig := &aws.Config{
		Region: aws.String(cfg.AWSRegion),
	}
	
	// Support custom endpoint via environment variable (for LocalStack)
	if endpoint := os.Getenv("AWS_ENDPOINT_URL"); endpoint != "" {
		awsConfig.Endpoint = aws.String(endpoint)
		awsConfig.S3ForcePathStyle = aws.Bool(true) // Required for LocalStack
		logger.Info("Using custom S3 endpoint", zap.String("endpoint", endpoint))
	}

	// Use session.NewSessionWithOptions to ensure credential chain is used
	sess, err := session.NewSessionWithOptions(session.Options{
		Config:            *awsConfig,
		SharedConfigState: session.SharedConfigEnable, // Enable ~/.aws/credentials
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create AWS session: %w", err)
	}

	s3Client := s3.New(sess)
	uploader := s3manager.NewUploader(sess, func(u *s3manager.Uploader) {
		u.PartSize = 10 * 1024 * 1024 // 10MB per part
		u.Concurrency = 3             // 3 concurrent uploads
	})

	return &Uploader{
		s3Client: s3Client,
		uploader: uploader,
		config:   cfg,
		logger:   logger,
	}, nil
}

// UploadFile uploads a file to S3 with automatic multipart for large files.
func (u *Uploader) UploadFile(filepath, s3Key string) error {
	file, err := os.Open(filepath)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	// Get file size
	fileInfo, err := file.Stat()
	if err != nil {
		return fmt.Errorf("failed to get file info: %w", err)
	}
	fileSize := fileInfo.Size()

	u.logger.Info("Uploading file to S3",
		zap.String("file", filepath),
		zap.String("s3_key", s3Key),
		zap.Int64("size", fileSize))

	// Use s3manager.Uploader which handles multipart automatically
	// It will use multipart upload for files > 5MB
	_, err = u.uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(u.config.S3Bucket),
		Key:    aws.String(s3Key),
		Body:   file,
	})

	if err != nil {
		return fmt.Errorf("failed to upload file: %w", err)
	}

	u.logger.Info("File uploaded successfully",
		zap.String("s3_key", s3Key),
		zap.Int64("size", fileSize))

	return nil
}

// UploadFileWithRetry uploads a file with retry logic.
func (u *Uploader) UploadFileWithRetry(filepath, s3Key string) error {
	var lastErr error
	delay := initialRetryDelay

	for attempt := 1; attempt <= maxS3Retries; attempt++ {
		err := u.UploadFile(filepath, s3Key)
		if err == nil {
			return nil
		}

		lastErr = err
		if attempt < maxS3Retries {
			u.logger.Warn("Upload failed, retrying",
				zap.String("file", filepath),
				zap.Int("attempt", attempt),
				zap.Int("max_retries", maxS3Retries),
				zap.Error(err))

			time.Sleep(delay)
			delay = time.Duration(float64(delay) * 2) // Exponential backoff
		}
	}

	return fmt.Errorf("upload failed after %d attempts: %w", maxS3Retries, lastErr)
}

// UploadMultipartFile uploads a large file using multipart upload (manual implementation).
// This is an alternative to s3manager.Uploader for more control.
func (u *Uploader) UploadMultipartFile(filepath, s3Key string) error {
	file, err := os.Open(filepath)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		return fmt.Errorf("failed to get file info: %w", err)
	}
	fileSize := fileInfo.Size()

	// For small files, use simple upload
	if fileSize < multipartThreshold {
		return u.UploadFile(filepath, s3Key)
	}

	u.logger.Info("Starting multipart upload",
		zap.String("file", filepath),
		zap.String("s3_key", s3Key),
		zap.Int64("size", fileSize))

	// Initiate multipart upload
	createInput := &s3.CreateMultipartUploadInput{
		Bucket: aws.String(u.config.S3Bucket),
		Key:    aws.String(s3Key),
	}

	createOutput, err := u.s3Client.CreateMultipartUpload(createInput)
	if err != nil {
		return fmt.Errorf("failed to create multipart upload: %w", err)
	}

	uploadID := createOutput.UploadId
	u.logger.Info("Multipart upload initiated",
		zap.String("upload_id", *uploadID))

	// Upload parts
	partSize := int64(10 * 1024 * 1024) // 10MB per part
	var parts []*s3.CompletedPart
	partNumber := int64(1)

	for {
		partData := make([]byte, partSize)
		n, err := file.Read(partData)
		if err == io.EOF && n == 0 {
			break
		}
		if err != nil && err != io.EOF {
			u.abortMultipartUpload(u.config.S3Bucket, s3Key, uploadID)
			return fmt.Errorf("failed to read file part: %w", err)
		}

		if n == 0 {
			break
		}

		// Upload part
		uploadPartInput := &s3.UploadPartInput{
			Bucket:     aws.String(u.config.S3Bucket),
			Key:        aws.String(s3Key),
			PartNumber: aws.Int64(partNumber),
			UploadId:   uploadID,
			Body:       bytes.NewReader(partData[:n]),
		}

		// Retry logic for part upload
		var partOutput *s3.UploadPartOutput
		for attempt := 1; attempt <= maxS3Retries; attempt++ {
			partOutput, err = u.s3Client.UploadPart(uploadPartInput)
			if err == nil {
				break
			}
			if attempt < maxS3Retries {
				u.logger.Warn("Part upload failed, retrying",
					zap.Int64("part", partNumber),
					zap.Int("attempt", attempt),
					zap.Error(err))
				time.Sleep(initialRetryDelay * time.Duration(attempt))
			}
		}

		if err != nil {
			u.abortMultipartUpload(u.config.S3Bucket, s3Key, uploadID)
			return fmt.Errorf("failed to upload part %d: %w", partNumber, err)
		}

		parts = append(parts, &s3.CompletedPart{
			ETag:       partOutput.ETag,
			PartNumber: aws.Int64(partNumber),
		})

		u.logger.Info("Part uploaded",
			zap.Int64("part", partNumber),
			zap.Int("size", n))

		partNumber++
	}

	// Complete multipart upload
	completeInput := &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(u.config.S3Bucket),
		Key:      aws.String(s3Key),
		UploadId: uploadID,
		MultipartUpload: &s3.CompletedMultipartUpload{
			Parts: parts,
		},
	}

	_, err = u.s3Client.CompleteMultipartUpload(completeInput)
	if err != nil {
		u.abortMultipartUpload(u.config.S3Bucket, s3Key, uploadID)
		return fmt.Errorf("failed to complete multipart upload: %w", err)
	}

	u.logger.Info("Multipart upload completed",
		zap.String("s3_key", s3Key),
		zap.Int64("parts", partNumber-1))

	return nil
}

// abortMultipartUpload aborts a multipart upload on error.
func (u *Uploader) abortMultipartUpload(bucket, key string, uploadID *string) {
	if uploadID == nil {
		return
	}

	abortInput := &s3.AbortMultipartUploadInput{
		Bucket:   aws.String(bucket),
		Key:      aws.String(key),
		UploadId: uploadID,
	}

	if _, err := u.s3Client.AbortMultipartUpload(abortInput); err != nil {
		u.logger.Error("Failed to abort multipart upload",
			zap.String("upload_id", *uploadID),
			zap.Error(err))
	} else {
		u.logger.Info("Aborted multipart upload",
			zap.String("upload_id", *uploadID))
	}
}

// MultipartUploadStream manages a streaming multipart upload where each batch is uploaded as a part.
// This is used for hash ranges where each 100k-row batch becomes a multipart part.
type MultipartUploadStream struct {
	uploader   *Uploader
	bucket     string
	key        string
	uploadID   *string
	parts      []*s3.CompletedPart
	partNumber int64
	logger     *zap.Logger
}

// NewMultipartUploadStream initiates a new multipart upload for streaming.
func (u *Uploader) NewMultipartUploadStream(s3Key string) (*MultipartUploadStream, error) {
	createInput := &s3.CreateMultipartUploadInput{
		Bucket: aws.String(u.config.S3Bucket),
		Key:    aws.String(s3Key),
	}

	createOutput, err := u.s3Client.CreateMultipartUpload(createInput)
	if err != nil {
		return nil, fmt.Errorf("failed to create multipart upload: %w", err)
	}

	u.logger.Info("Initiated multipart upload stream",
		zap.String("s3_key", s3Key),
		zap.String("upload_id", *createOutput.UploadId))

	return &MultipartUploadStream{
		uploader:   u,
		bucket:     u.config.S3Bucket,
		key:        s3Key,
		uploadID:   createOutput.UploadId,
		parts:      []*s3.CompletedPart{},
		partNumber: 1,
		logger:     u.logger,
	}, nil
}

// UploadPart uploads a batch of data as a multipart part.
// The data should be CSV content (can be a batch of rows).
func (m *MultipartUploadStream) UploadPart(data []byte) error {
	if len(data) == 0 {
		return nil // Skip empty parts
	}

	uploadPartInput := &s3.UploadPartInput{
		Bucket:     aws.String(m.bucket),
		Key:        aws.String(m.key),
		PartNumber: aws.Int64(m.partNumber),
		UploadId:   m.uploadID,
		Body:       bytes.NewReader(data),
	}

	// Retry logic for part upload
	var partOutput *s3.UploadPartOutput
	var err error
	for attempt := 1; attempt <= maxS3Retries; attempt++ {
		partOutput, err = m.uploader.s3Client.UploadPart(uploadPartInput)
		if err == nil {
			break
		}
		if attempt < maxS3Retries {
			m.logger.Warn("Part upload failed, retrying",
				zap.Int64("part", m.partNumber),
				zap.Int("attempt", attempt),
				zap.Error(err))
			time.Sleep(initialRetryDelay * time.Duration(attempt))
		}
	}

	if err != nil {
		m.uploader.abortMultipartUpload(m.bucket, m.key, m.uploadID)
		return fmt.Errorf("failed to upload part %d: %w", m.partNumber, err)
	}

	m.parts = append(m.parts, &s3.CompletedPart{
		ETag:       partOutput.ETag,
		PartNumber: aws.Int64(m.partNumber),
	})

	m.logger.Info("Uploaded multipart part",
		zap.Int64("part", m.partNumber),
		zap.Int("size", len(data)))

	m.partNumber++
	return nil
}

// Complete finalizes the multipart upload after all parts have been uploaded.
func (m *MultipartUploadStream) Complete() error {
	if len(m.parts) == 0 {
		// No parts uploaded, abort the upload
		m.uploader.abortMultipartUpload(m.bucket, m.key, m.uploadID)
		return fmt.Errorf("no parts uploaded")
	}

	completeInput := &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(m.bucket),
		Key:      aws.String(m.key),
		UploadId: m.uploadID,
		MultipartUpload: &s3.CompletedMultipartUpload{
			Parts: m.parts,
		},
	}

	_, err := m.uploader.s3Client.CompleteMultipartUpload(completeInput)
	if err != nil {
		m.uploader.abortMultipartUpload(m.bucket, m.key, m.uploadID)
		return fmt.Errorf("failed to complete multipart upload: %w", err)
	}

	m.logger.Info("Completed multipart upload",
		zap.String("s3_key", m.key),
		zap.Int64("parts", m.partNumber-1))

	return nil
}

// Abort cancels the multipart upload.
func (m *MultipartUploadStream) Abort() {
	m.uploader.abortMultipartUpload(m.bucket, m.key, m.uploadID)
}

