// Dedupe.go will hash the contents of all files in the supplied input directories
// and produce hard links to them, with the name set hash, in the output directory.
// This way any duplicate files will only have a single hard link.
// One this has run, the input directories can be deleted as the hard links will
// precent the actual data being deleted.

package main

import (
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/alecthomas/kingpin"
	sha256 "github.com/minio/sha256-simd"
)

var (
	parallelism = kingpin.Flag("parallelism", "").Default("16").Int()
	extension   = kingpin.Flag("extension", "").Default("(jpg|jpeg|tiff|png|avi|mpg|mp4|mov|3gp)").String()
	skipFile    = kingpin.Flag("skip-file", "").Default(`\.jpg_face(\d+)\.`).String()
	skipDir     = kingpin.Flag("skip-dir", "").Default("^(Thumbnails|derivatives|Previews|face)$").String()
	output      = kingpin.Arg("ouput", "").ExistingDir()
	input       = kingpin.Arg("input", "").ExistingDirs()
)

func main() {
	kingpin.Parse()

	queue := make(chan string)
	var wg sync.WaitGroup
	wg.Add(*parallelism)

	var files int32
	var skipped int32
	var duplicates int32

	skipDirRE, err := regexp.Compile(*skipDir)
	if err != nil {
		log.Fatal(err)
	}

	skipFileRE, err := regexp.Compile(*skipFile)
	if err != nil {
		log.Fatal(err)
	}

	extensionRE, err := regexp.Compile(*extension)
	if err != nil {
		log.Fatal(err)
	}

	for i := 0; i < *parallelism; i++ {
		go func() {
			defer wg.Done()
			for path := range queue {
				hash, err := hashFile(path)
				if err != nil {
					log.Fatal(err)
				}

				ext := strings.ToLower(filepath.Ext(path))
				dirpart, filepart := hash[:1], hash[1:]
				dirname := filepath.Join(*output, fmt.Sprintf("%x", dirpart))
				filename := filepath.Join(dirname, fmt.Sprintf("%x%s", filepart, ext))
				log.Printf("%s -> %s", filename, path)

				if err := os.Mkdir(dirname, 0777); err != nil && !os.IsExist(err) {
					log.Fatal(err)
				}

				if err := os.Link(path, filename); err != nil {
					if os.IsExist(err) {
						atomic.AddInt32(&duplicates, 1)
					} else {
						log.Fatal(err)
					}
				}
			}
		}()
	}

	for _, dir := range *input {
		filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}

			if info.IsDir() {
				if skipDirRE.MatchString(info.Name()) {
					skipped++
					log.Printf("skipping %s", path)
					return filepath.SkipDir
				}
				return nil
			}

			ext := strings.ToLower(filepath.Ext(path))
			if !extensionRE.MatchString(ext) {
				skipped++
				log.Printf("skipping %s", path)
				return nil
			}

			if skipFileRE.MatchString(info.Name()) {
				skipped++
				log.Printf("skipping %s", path)
				return nil
			}

			files++
			queue <- path
			return nil
		})
	}

	close(queue)
	wg.Wait()
	fmt.Printf("Skipped: %d, Files: %d, Duplicates: %d\n", skipped, files, duplicates)
}

func hashFile(path string) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return nil, err
	}

	return hash.Sum(nil), nil
}
