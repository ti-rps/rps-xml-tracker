// fsscan — Phase-0 (F0.2/F0.3) READ-ONLY investigation tool.
//
// Walks a sample directory of the XML flow (e.g. C:\xml_asincronizar or
// C:\xml_sincronizado on srvdoc01), and reports:
//   - volume: file counts, total bytes, depth of subfolders
//   - taxonomy: document-type variants (NFe / nfeProc / resNFe / eventos / NFCe / ...)
//   - chave extraction: how many files yield a 44-digit chave, and how
//   - parse cost: time spent extracting the chave (to size the agent)
//
// It NEVER writes, moves, renames, or locks any file. It opens files
// read-only and only reads their content. This mirrors the hard read-only
// rule of the tracker.
//
// Usage:
//   go run ./fsscan -root "C:\\xml_asincronizar" [-sample 5000] [-json out.json] [-show-unknown]
package main

import (
	"encoding/json"
	"encoding/xml"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

type fileResult struct {
	Path      string
	Ext       string
	SizeBytes int64
	Depth     int
	RootElem  string // local name of the XML root element
	DocType   string // classified document type
	Mod       string // 55 / 65 when present
	Chave     string // 44-digit access key, when found
	ChaveVia  string // how the chave was found: "infNFe@Id" | "chNFe" | "chCTe" | ""
	ParseNs   int64  // nanoseconds spent parsing this file
	Err       string
}

type summary struct {
	Root             string            `json:"root"`
	ScannedAt        string            `json:"scanned_at"`
	TotalFiles       int               `json:"total_files"`
	XMLFiles         int               `json:"xml_files"`
	NonXMLFiles      int               `json:"non_xml_files"`
	ParsedOK         int               `json:"parsed_ok"`
	ParseErrors      int               `json:"parse_errors"`
	WithChave        int               `json:"with_chave"`
	WithoutChave     int               `json:"without_chave"`
	TotalBytes       int64             `json:"total_bytes"`
	MaxDepth         int               `json:"max_depth"`
	ByExt            map[string]int    `json:"by_ext"`
	ByDocType        map[string]int    `json:"by_doc_type"`
	ByRootElem       map[string]int    `json:"by_root_elem"`
	ByChaveVia       map[string]int    `json:"by_chave_via"`
	ParseNsP50       int64             `json:"parse_ns_p50"`
	ParseNsP95       int64             `json:"parse_ns_p95"`
	ParseNsMax       int64             `json:"parse_ns_max"`
	SampleUnknown    []string          `json:"sample_unknown,omitempty"`
	SampleNoChave    []string          `json:"sample_no_chave,omitempty"`
	WallSeconds      float64           `json:"wall_seconds"`
}

func main() {
	root := flag.String("root", "", "directory to scan (read-only) — required")
	sample := flag.Int("sample", 0, "stop after parsing N xml files (0 = no limit)")
	jsonOut := flag.String("json", "", "write full summary JSON to this path")
	showUnknown := flag.Bool("show-unknown", false, "print sample paths of unknown/no-chave files")
	flag.Parse()

	if *root == "" {
		fmt.Fprintln(os.Stderr, "error: -root is required")
		flag.Usage()
		os.Exit(2)
	}

	start := time.Now()
	rootClean := filepath.Clean(*root)
	rootDepth := strings.Count(rootClean, string(os.PathSeparator))

	sum := summary{
		Root:       rootClean,
		ScannedAt:  start.Format(time.RFC3339),
		ByExt:      map[string]int{},
		ByDocType:  map[string]int{},
		ByRootElem: map[string]int{},
		ByChaveVia: map[string]int{},
	}
	var parseTimes []int64
	var unknownSamples, noChaveSamples []string

	walkErr := filepath.WalkDir(rootClean, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			// unreadable entry — record and continue, never abort
			sum.ParseErrors++
			return nil
		}
		if d.IsDir() {
			depth := strings.Count(filepath.Clean(path), string(os.PathSeparator)) - rootDepth
			if depth > sum.MaxDepth {
				sum.MaxDepth = depth
			}
			return nil
		}
		sum.TotalFiles++
		if sum.TotalFiles%20000 == 0 {
			// progress heartbeat to stderr (keeps stdout/JSON report clean)
			fmt.Fprintf(os.Stderr, "  ...scanned %d files (xml %d, parsed %d, with-chave %d) %.0fs\n",
				sum.TotalFiles, sum.XMLFiles, sum.ParsedOK, sum.WithChave, time.Since(start).Seconds())
		}
		ext := strings.ToLower(filepath.Ext(path))
		sum.ByExt[ext]++

		info, ierr := d.Info()
		if ierr == nil {
			sum.TotalBytes += info.Size()
		}

		if ext != ".xml" {
			sum.NonXMLFiles++
			return nil
		}
		sum.XMLFiles++

		if *sample > 0 && sum.ParsedOK+sum.ParseErrors >= *sample {
			return filepath.SkipAll
		}

		res := parseFile(path)
		if res.Err != "" {
			sum.ParseErrors++
			return nil
		}
		sum.ParsedOK++
		sum.ByDocType[orUnknown(res.DocType)]++
		sum.ByRootElem[orUnknown(res.RootElem)]++
		parseTimes = append(parseTimes, res.ParseNs)

		if res.Chave != "" {
			sum.WithChave++
			sum.ByChaveVia[res.ChaveVia]++
		} else {
			sum.WithoutChave++
			if len(noChaveSamples) < 25 {
				noChaveSamples = append(noChaveSamples, fmt.Sprintf("%s (root=%s)", path, res.RootElem))
			}
		}
		if res.DocType == "unknown" && len(unknownSamples) < 25 {
			unknownSamples = append(unknownSamples, fmt.Sprintf("%s (root=%s)", path, res.RootElem))
		}
		return nil
	})
	if walkErr != nil {
		fmt.Fprintf(os.Stderr, "walk error: %v\n", walkErr)
	}

	sum.ParseNsP50, sum.ParseNsP95, sum.ParseNsMax = percentiles(parseTimes)
	sum.WallSeconds = time.Since(start).Seconds()
	if *showUnknown {
		sum.SampleUnknown = unknownSamples
		sum.SampleNoChave = noChaveSamples
	}

	printReport(sum)

	if *jsonOut != "" {
		b, _ := json.MarshalIndent(sum, "", "  ")
		if err := os.WriteFile(*jsonOut, b, 0o644); err != nil {
			fmt.Fprintf(os.Stderr, "could not write json: %v\n", err)
		} else {
			fmt.Printf("\nFull JSON written to %s\n", *jsonOut)
		}
	}
}

// parseFile streams the XML (read-only) to classify the document and extract
// the chave de acesso. It does not load the whole file as a DOM and stops as
// soon as it has both the root element and a chave.
func parseFile(path string) fileResult {
	res := fileResult{Path: path, Ext: strings.ToLower(filepath.Ext(path))}
	t0 := time.Now()
	defer func() { res.ParseNs = time.Since(t0).Nanoseconds() }()

	f, err := os.Open(path) // read-only
	if err != nil {
		res.Err = err.Error()
		return res
	}
	defer f.Close()

	dec := xml.NewDecoder(f)
	dec.Strict = false // tolerate the messy real-world XML we'll meet
	dec.CharsetReader = func(_ string, input io.Reader) (io.Reader, error) {
		return input, nil // accept latin1/etc. bytes as-is; we only need ASCII keys
	}

	var inChNFe, inChCTe bool
	for {
		tok, err := dec.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			if res.RootElem == "" {
				res.Err = err.Error()
			}
			break
		}
		switch t := tok.(type) {
		case xml.StartElement:
			local := t.Name.Local
			if res.RootElem == "" {
				res.RootElem = local
				res.DocType = classify(local)
			}
			switch local {
			case "infNFe", "infNFCe":
				for _, a := range t.Attr {
					if a.Name.Local == "Id" && res.Chave == "" {
						res.Chave = normalizeKey(a.Value)
						if res.Chave != "" {
							res.ChaveVia = "infNFe@Id"
						}
					}
				}
			case "mod":
				// read the model code (55/65)
				if v, ok := readCharData(dec); ok {
					res.Mod = strings.TrimSpace(v)
				}
			case "chNFe":
				inChNFe = true
			case "chCTe":
				inChCTe = true
			}
		case xml.CharData:
			if inChNFe && res.Chave == "" {
				if k := normalizeKey(string(t)); k != "" {
					res.Chave = k
					res.ChaveVia = "chNFe"
				}
			} else if inChCTe && res.Chave == "" {
				if k := normalizeKey(string(t)); k != "" {
					res.Chave = k
					res.ChaveVia = "chCTe"
				}
			}
		case xml.EndElement:
			switch t.Name.Local {
			case "chNFe":
				inChNFe = false
			case "chCTe":
				inChCTe = false
			}
		}
		// early exit once we have a chave and know the root/type
		if res.Chave != "" && res.RootElem != "" && res.Mod != "" {
			break
		}
	}
	return res
}

func readCharData(dec *xml.Decoder) (string, bool) {
	tok, err := dec.Token()
	if err != nil {
		return "", false
	}
	if cd, ok := tok.(xml.CharData); ok {
		return string(cd), true
	}
	return "", false
}

// normalizeKey strips a leading "NFe"/"CTe" prefix and any non-digits, then
// returns the value only if it is exactly 44 digits.
func normalizeKey(s string) string {
	s = strings.TrimSpace(s)
	s = strings.TrimPrefix(s, "NFe")
	s = strings.TrimPrefix(s, "CTe")
	var b strings.Builder
	for _, r := range s {
		if r >= '0' && r <= '9' {
			b.WriteRune(r)
		}
	}
	d := b.String()
	if len(d) == 44 {
		return d
	}
	return ""
}

// classify maps a root element local name to a coarse document type.
func classify(root string) string {
	switch root {
	case "nfeProc":
		return "procNFe"
	case "NFe":
		return "NFe"
	case "enviNFe":
		return "enviNFe"
	case "resNFe":
		return "resNFe"
	case "resEvento":
		return "resEvento"
	case "procEventoNFe", "envEvento", "evento", "retEvento":
		return "eventoNFe"
	case "cteProc", "CTe":
		return "CTe"
	case "ConsultaNFe", "ConsSitNFe":
		return "consulta"
	default:
		return "unknown"
	}
}

func orUnknown(s string) string {
	if s == "" {
		return "unknown"
	}
	return s
}

func percentiles(v []int64) (p50, p95, max int64) {
	if len(v) == 0 {
		return 0, 0, 0
	}
	sort.Slice(v, func(i, j int) bool { return v[i] < v[j] })
	idx := func(p float64) int {
		i := int(p * float64(len(v)))
		if i >= len(v) {
			i = len(v) - 1
		}
		return i
	}
	return v[idx(0.50)], v[idx(0.95)], v[len(v)-1]
}

func printReport(s summary) {
	fmt.Printf("== fsscan (READ-ONLY) — %s ==\n", s.Root)
	fmt.Printf("Scanned at:    %s  (wall %.2fs)\n", s.ScannedAt, s.WallSeconds)
	fmt.Printf("Total files:   %d  (xml %d, non-xml %d)\n", s.TotalFiles, s.XMLFiles, s.NonXMLFiles)
	fmt.Printf("Total bytes:   %s\n", humanBytes(s.TotalBytes))
	fmt.Printf("Max depth:     %d subfolder level(s)\n", s.MaxDepth)
	fmt.Printf("Parsed OK:     %d   Parse errors: %d\n", s.ParsedOK, s.ParseErrors)
	fmt.Printf("With chave:    %d   Without chave: %d", s.WithChave, s.WithoutChave)
	if s.ParsedOK > 0 {
		fmt.Printf("   (%.1f%% identifiable)", 100*float64(s.WithChave)/float64(s.ParsedOK))
	}
	fmt.Println()
	fmt.Printf("Parse cost:    p50=%s  p95=%s  max=%s\n",
		dur(s.ParseNsP50), dur(s.ParseNsP95), dur(s.ParseNsMax))

	printMap("Document types", s.ByDocType)
	printMap("Root elements", s.ByRootElem)
	printMap("Chave found via", s.ByChaveVia)
	printMap("Extensions", s.ByExt)

	if len(s.SampleUnknown) > 0 {
		fmt.Println("\nSample UNKNOWN-type files:")
		for _, p := range s.SampleUnknown {
			fmt.Printf("  - %s\n", p)
		}
	}
	if len(s.SampleNoChave) > 0 {
		fmt.Println("\nSample files with NO chave:")
		for _, p := range s.SampleNoChave {
			fmt.Printf("  - %s\n", p)
		}
	}
}

func printMap(title string, m map[string]int) {
	if len(m) == 0 {
		return
	}
	type kv struct {
		k string
		v int
	}
	var items []kv
	for k, v := range m {
		items = append(items, kv{k, v})
	}
	sort.Slice(items, func(i, j int) bool { return items[i].v > items[j].v })
	fmt.Printf("\n%s:\n", title)
	for _, it := range items {
		fmt.Printf("  %-16s %d\n", it.k, it.v)
	}
}

func humanBytes(b int64) string {
	const u = 1024
	if b < u {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(u), 0
	for n := b / u; n >= u; n /= u {
		div *= u
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(b)/float64(div), "KMGTPE"[exp])
}

func dur(ns int64) string { return time.Duration(ns).String() }
