package decoder

// splitterとerasurecodeのインターフェースを実装する
type Decoder interface {
	// DecodeAndMergeは、分割されたファイルをデコードしてマージします。
	// inputFilesは、分割されたファイルのパスのスライスです。
	// outputFileは、マージされたファイルの出力先パスです。
	// 返り値は、エラーが発生した場合にそのエラーを返します。
	DecodeAndMerge(inputFiles []string, outputFile string) error
	// Decodeは、分割されたファイルをデコードします。
	// inputFilesは、分割されたファイルのパスのスライスです。
	// 返り値は、デコードされたデータとエラーが発生した場合にそのエラーを返します。
	// デコードされたデータは、バイトスライスとして返されます。
	// 返り値のエラーは、デコードに失敗した場合に返されます。
	// 返り値のデータは、デコードされたデータを含みます。
	// 返り値のエラーは、デコードに失敗した場合に返されます。
	Decode(inputFiles []string) ([]byte, error)
}

// DecodeAndMergeの実装

