package auth

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"
)

// VerifySignature はHTTPリクエストのAWS SigV4署名を検証します。
// accessKeyとsecretKeyは、検証に使用する認証情報です。
func VerifySignature(r *http.Request, accessKey, secretKey string) error {
	fmt.Printf("INFO: Verifying signature for request: %s %s\n", r.Method, r.URL.Path)

	// 1. Canonical Request の構築
	canonicalRequest, err := getCanonicalRequest(r)
	if err != nil {
		fmt.Printf("ERROR: Failed to get canonical request: %v\n", err)
		return fmt.Errorf("failed to get canonical request: %w", err)
	}
	fmt.Printf("INFO: Canonical Request: %s\n", canonicalRequest)

	// 2. String to Sign の構築
	canonicalRequestHash := sha256Hash(canonicalRequest)
	fmt.Printf("INFO: Canonical Request Hash: %s\n", canonicalRequestHash)
	stringToSign, err := getStringToSign(r, canonicalRequestHash)
	if err != nil {
		fmt.Printf("ERROR: Failed to get string to sign: %v\n", err)
		return fmt.Errorf("failed to get string to sign: %w", err)
	}
	fmt.Printf("INFO: String to Sign: %s\n", stringToSign)

	// 3. Signing Key の導出
	date := r.Header.Get("x-amz-date")
	if date == "" {
		fmt.Printf("ERROR: x-amz-date header is missing\n")
		return fmt.Errorf("x-amz-date header is missing")
	}
	fmt.Printf("INFO: x-amz-date: %s\n", date)

	parsedTime, err := time.Parse("20060102T150405Z", date)
	if err != nil {
		fmt.Printf("ERROR: Invalid x-amz-date format: %v\n", err)
		return fmt.Errorf("invalid x-amz-date format: %w", err)
	}

	region := "us-east-1" // 仮のリージョン
	service := "s3"       // 仮のサービス
	fmt.Printf("INFO: Using region: %s, service: %s\n", region, service)

	signingKey, err := getSignatureKey(secretKey, parsedTime.Format("20060102"), region, service)
	if err != nil {
		fmt.Printf("ERROR: Failed to get signing key: %v\n", err)
		return fmt.Errorf("failed to get signing key: %w", err)
	}
	fmt.Printf("INFO: Signing Key derived.\n")

	// 4. 署名の計算
	calculatedSignature := calculateSignature(signingKey, stringToSign)
	fmt.Printf("INFO: Calculated Signature: %s\n", calculatedSignature)

	// 5. 署名の比較
	authHeader := r.Header.Get("Authorization")
	if authHeader == "" {
		fmt.Printf("ERROR: Authorization header is missing\n")
		return fmt.Errorf("Authorization header is missing")
	}
	fmt.Printf("INFO: Authorization Header: %s\n", authHeader)

	providedSignature, err := extractSignatureFromAuthHeader(authHeader)
	if err != nil {
		fmt.Printf("ERROR: Failed to extract signature from Authorization header: %v\n", err)
		return fmt.Errorf("failed to extract signature from Authorization header: %w", err)
	}
	fmt.Printf("INFO: Provided Signature: %s\n", providedSignature)

	if calculatedSignature != providedSignature {
		fmt.Printf("ERROR: Signature mismatch: calculated %s, provided %s\n", calculatedSignature, providedSignature)
		return fmt.Errorf("signature mismatch: calculated %s, provided %s", calculatedSignature, providedSignature)
	}

	fmt.Printf("INFO: Signature verification successful.\n")
	return nil
}

// getCanonicalRequest はCanonical Requestを構築します。
func getCanonicalRequest(r *http.Request) (string, error) {
	fmt.Printf("DEBUG: Entering getCanonicalRequest\n")
	// HTTPMethod
	method := r.Method
	fmt.Printf("DEBUG: HTTP Method: %s\n", method)

	// CanonicalURI
	canonicalURI := r.URL.Path
	if canonicalURI == "" {
		canonicalURI = "/"
	}
	fmt.Printf("DEBUG: Canonical URI: %s\n", canonicalURI)

	// CanonicalQueryString
	var queryParts []string
	for key, values := range r.URL.Query() {
		for _, value := range values {
			queryParts = append(queryParts, fmt.Sprintf("%s=%s", escape(key), escape(value)))
		}
	}
	sort.Strings(queryParts)
	canonicalQueryString := strings.Join(queryParts, "&")
	fmt.Printf("DEBUG: Canonical Query String: %s\n", canonicalQueryString)

	// CanonicalHeaders
	var headerParts []string
	headersToSign := []string{"host", "x-amz-date"} // 署名対象のヘッダー
	sort.Strings(headersToSign)
	fmt.Printf("DEBUG: Headers to Sign: %v\n", headersToSign)

	for _, headerName := range headersToSign {
		value := strings.TrimSpace(r.Header.Get(headerName))
		headerParts = append(headerParts, fmt.Sprintf("%s:%s", headerName, value))
	}
	canonicalHeaders := strings.Join(headerParts, "\n") + "\n"
	fmt.Printf("DEBUG: Canonical Headers: %s\n", canonicalHeaders)

	// SignedHeaders
	signedHeaders := strings.Join(headersToSign, ";")
	fmt.Printf("DEBUG: Signed Headers: %s\n", signedHeaders)

	// HashedPayload
	payloadHash := r.Header.Get("x-amz-content-sha256")
	if payloadHash == "" {
		fmt.Printf("ERROR: x-amz-content-sha256 header is missing in getCanonicalRequest\n")
		return "", fmt.Errorf("x-amz-content-sha256 header is missing")
	}
	fmt.Printf("DEBUG: Payload Hash: %s\n", payloadHash)

	canonicalRequest := fmt.Sprintf("%s\n%s\n%s\n%s\n%s\n%s",
		method,
		canonicalURI,
		canonicalQueryString,
		canonicalHeaders,
		signedHeaders,
		payloadHash,
	)
	fmt.Printf("DEBUG: Exiting getCanonicalRequest\n")
	return canonicalRequest, nil
}

// getStringToSign はString to Signを構築します。
func getStringToSign(r *http.Request, canonicalRequestHash string) (string, error) {
	fmt.Printf("DEBUG: Entering getStringToSign\n")
	date := r.Header.Get("x-amz-date")
	if date == "" {
		fmt.Printf("ERROR: x-amz-date header is missing in getStringToSign\n")
		return "", fmt.Errorf("x-amz-date header is missing")
	}
	fmt.Printf("DEBUG: x-amz-date in getStringToSign: %s\n", date)

	parsedTime, err := time.Parse("20060102T150405Z", date)
	if err != nil {
		fmt.Printf("ERROR: Invalid x-amz-date format in getStringToSign: %v\n", err)
		return "", fmt.Errorf("invalid x-amz-date format: %w", err)
	}

	credentialScope := fmt.Sprintf("%s/%s/%s/aws4_request", parsedTime.Format("20060102"), "us-east-1", "s3")
	fmt.Printf("DEBUG: Credential Scope: %s\n", credentialScope)

	stringToSign := fmt.Sprintf("AWS4-HMAC-SHA256\n%s\n%s\n%s",
		date,
		credentialScope,
		canonicalRequestHash,
	)
	fmt.Printf("DEBUG: Exiting getStringToSign\n")
	return stringToSign, nil
}

// getSignatureKey は署名キーを導出します。
func getSignatureKey(secretKey, date, region, service string) ([]byte, error) {
	fmt.Printf("DEBUG: Entering getSignatureKey for date: %s, region: %s, service: %s\n", date, region, service)
	kDate := hmacSHA256([]byte("AWS4"+secretKey), []byte(date))
	kRegion := hmacSHA256(kDate, []byte(region))
	kService := hmacSHA256(kRegion, []byte(service))
	kSigning := hmacSHA256(kService, []byte("aws4_request"))
	fmt.Printf("DEBUG: Exiting getSignatureKey\n")
	return kSigning, nil
}

// calculateSignature は署名を計算します。
func calculateSignature(signingKey []byte, stringToSign string) string {
	fmt.Printf("DEBUG: Entering calculateSignature\n")
	h := hmacSHA256(signingKey, []byte(stringToSign))
	sig := hex.EncodeToString(h)
	fmt.Printf("DEBUG: Calculated signature (raw): %s\n", sig)
	fmt.Printf("DEBUG: Exiting calculateSignature\n")
	return sig
}

// hmacSHA256 はHMAC-SHA256ハッシュを計算します。
func hmacSHA256(key, data []byte) []byte {
	// This is a low-level helper, no need for extensive logging here unless debugging specific crypto issues.
	h := hmac.New(sha256.New, key)
	h.Write(data)
	return h.Sum(nil)
}

// sha256Hash は文字列のSHA256ハッシュを計算し、16進数文字列で返します。
func sha256Hash(data string) string {
	// This is a low-level helper, no need for extensive logging here unless debugging specific crypto issues.
	h := sha256.New()
	h.Write([]byte(data))
	return hex.EncodeToString(h.Sum(nil))
}

// escape はURLエンコードを行います。
func escape(s string) string {
	fmt.Printf("DEBUG: Escaping string: %s\n", s)
	// TODO: AWS SigV4の特定のエンコードルールに従う
	// 現時点では、net/url.QueryEscapeに似た挙動を想定
	escaped := strings.ReplaceAll(strings.ReplaceAll(strings.ReplaceAll(s, "~", "%7E"), " ", "%20"), "+", "%2B")
	fmt.Printf("DEBUG: Escaped string: %s\n", escaped)
	return escaped
}

// extractSignatureFromAuthHeader はAuthorizationヘッダーから署名を抽出します。
func extractSignatureFromAuthHeader(authHeader string) (string, error) {
	fmt.Printf("DEBUG: Entering extractSignatureFromAuthHeader with header: %s\n", authHeader)
	parts := strings.Split(authHeader, ", ")
	for _, part := range parts {
		if strings.HasPrefix(part, "Signature=") {
			sig := strings.TrimPrefix(part, "Signature=")
			fmt.Printf("DEBUG: Extracted signature: %s\n", sig)
			fmt.Printf("DEBUG: Exiting extractSignatureFromAuthHeader\n")
			return sig, nil
		}
	}
	fmt.Printf("ERROR: Signature not found in Authorization header: %s\n", authHeader)
	return "", fmt.Errorf("signature not found in Authorization header")
}
