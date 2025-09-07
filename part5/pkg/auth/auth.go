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
	// 1. Canonical Request の構築
	canonicalRequest, err := getCanonicalRequest(r)
	if err != nil {
		return fmt.Errorf("failed to get canonical request: %w", err)
	}

	// 2. String to Sign の構築
	canonicalRequestHash := sha256Hash(canonicalRequest)
	stringToSign, err := getStringToSign(r, canonicalRequestHash)
	if err != nil {
		return fmt.Errorf("failed to get string to sign: %w", err)
	}

	// 3. Signing Key の導出
	// TODO: リージョンとサービス名をリクエストから抽出するか、設定で渡す
	// 現時点では仮の値を使用

date := r.Header.Get("x-amz-date")
	if date == "" {
		return fmt.Errorf("x-amz-date header is missing")
	}

	// 日付のフォーマットはYYYYMMDDTHHMMSSZ
	parsedTime, err := time.Parse("20060102T150405Z", date)
	if err != nil {
		return fmt.Errorf("invalid x-amz-date format: %w", err)
	}

	region := "us-east-1" // 仮のリージョン
	service := "s3"      // 仮のサービス

	signingKey, err := getSignatureKey(secretKey, parsedTime.Format("20060102"), region, service)
	if err != nil {
		return fmt.Errorf("failed to get signing key: %w", err)
	}

	// 4. 署名の計算
	calculatedSignature := calculateSignature(signingKey, stringToSign)

	// 5. 署名の比較
	authHeader := r.Header.Get("Authorization")
	if authHeader == "" {
		return fmt.Errorf("Authorization header is missing")
	}

	// Authorizationヘッダーから提供された署名を抽出
	providedSignature, err := extractSignatureFromAuthHeader(authHeader)
	if err != nil {
		return fmt.Errorf("failed to extract signature from Authorization header: %w", err)
	}

	if calculatedSignature != providedSignature {
		return fmt.Errorf("signature mismatch: calculated %s, provided %s", calculatedSignature, providedSignature)
	}

	return nil
}

// getCanonicalRequest はCanonical Requestを構築します。
func getCanonicalRequest(r *http.Request) (string, error) {
	// HTTPMethod
	method := r.Method

	// CanonicalURI
	// クエリパラメータは含まない
	canonicalURI := r.URL.Path
	if canonicalURI == "" {
		canonicalURI = "/"
	}

	// CanonicalQueryString
	// クエリパラメータをソートしてエンコード
	var queryParts []string
	for key, values := range r.URL.Query() {
		for _, value := range values {
			queryParts = append(queryParts, fmt.Sprintf("%s=%s", escape(key), escape(value)))
		}
	}
	sort.Strings(queryParts)
	canonicalQueryString := strings.Join(queryParts, "&")

	// CanonicalHeaders
	// ヘッダー名を小文字にし、ソートして結合
	var headerParts []string
	headersToSign := []string{"host", "x-amz-date"} // 署名対象のヘッダー
	sort.Strings(headersToSign)

	for _, headerName := range headersToSign {
		value := strings.TrimSpace(r.Header.Get(headerName))
		headerParts = append(headerParts, fmt.Sprintf("%s:%s", headerName, value))
	}
	canonicalHeaders := strings.Join(headerParts, "\n") + "\n"

	// SignedHeaders
	// 署名対象のヘッダー名をソートしてセミコロンで結合
	signedHeaders := strings.Join(headersToSign, ";")

	// HashedPayload
	// リクエストボディのSHA256ハッシュ
	payloadHash := r.Header.Get("x-amz-content-sha256")
	if payloadHash == "" {
		// x-amz-content-sha256がない場合は、ボディのハッシュを計算
		// TODO: ボディを読み込むと、後続のハンドラで読み込めなくなる可能性があるので注意
		// 現時点では、テスト用に空のハッシュを返すか、エラーを返す
		return "", fmt.Errorf("x-amz-content-sha256 header is missing")
	}

	canonicalRequest := fmt.Sprintf("%s\n%s\n%s\n%s\n%s\n%s",
		method,
		canonicalURI,
		canonicalQueryString,
		canonicalHeaders,
		signedHeaders,
		payloadHash,
	)

	return canonicalRequest, nil
}

// getStringToSign はString to Signを構築します。
func getStringToSign(r *http.Request, canonicalRequestHash string) (string, error) {
	// TODO: Credential Scope の構築
	// 現時点では仮の値を使用

date := r.Header.Get("x-amz-date")
	if date == "" {
		return "", fmt.Errorf("x-amz-date header is missing")
	}

	parsedTime, err := time.Parse("20060102T150405Z", date)
	if err != nil {
		return "", fmt.Errorf("invalid x-amz-date format: %w", err)
	}

	credentialScope := fmt.Sprintf("%s/%s/%s/aws4_request", parsedTime.Format("20060102"), "us-east-1", "s3")

	stringToSign := fmt.Sprintf("AWS4-HMAC-SHA256\n%s\n%s\n%s",
		date,
		credentialScope,
		canonicalRequestHash,
	)

	return stringToSign, nil
}

// getSignatureKey は署名キーを導出します。
func getSignatureKey(secretKey, date, region, service string) ([]byte, error) {
	kDate := hmacSHA256([]byte("AWS4"+secretKey), []byte(date))
	kRegion := hmacSHA256(kDate, []byte(region))
	kService := hmacSHA256(kRegion, []byte(service))
	kSigning := hmacSHA256(kService, []byte("aws4_request"))
	return kSigning, nil
}

// calculateSignature は署名を計算します。
func calculateSignature(signingKey []byte, stringToSign string) string {
	h := hmacSHA256(signingKey, []byte(stringToSign))
	return hex.EncodeToString(h)
}

// hmacSHA256 はHMAC-SHA256ハッシュを計算します。
func hmacSHA256(key, data []byte) []byte {
	h := hmac.New(sha256.New, key)
	h.Write(data)
	return h.Sum(nil)
}

// sha256Hash は文字列のSHA256ハッシュを計算し、16進数文字列で返します。
func sha256Hash(data string) string {
	h := sha256.New()
	h.Write([]byte(data))
	return hex.EncodeToString(h.Sum(nil))
}

// escape はURLエンコードを行います。
func escape(s string) string {
	// TODO: AWS SigV4の特定のエンコードルールに従う
	// 現時点では、net/url.QueryEscapeに似た挙動を想定
	return strings.ReplaceAll(strings.ReplaceAll(strings.ReplaceAll(s, "~", "%7E"), " ", "%20"), "+", "%2B")
}

// extractSignatureFromAuthHeader はAuthorizationヘッダーから署名を抽出します。
func extractSignatureFromAuthHeader(authHeader string) (string, error) {
	parts := strings.Split(authHeader, ", ")
	for _, part := range parts {
		if strings.HasPrefix(part, "Signature=") {
			return strings.TrimPrefix(part, "Signature="), nil
		}
	}
	return "", fmt.Errorf("signature not found in Authorization header")
}
