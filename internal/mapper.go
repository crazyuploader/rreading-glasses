package internal

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
)

type bridge interface {
	// MapAuthor maps a GR author ID to a foreign author ID.
	MapAuthor(ctx context.Context, hc *HCGetter, grAuthorID int64) (string, error)

	MapWork(ctx context.Context, grWorkID int64) (*workResource, error)

	MapEdition(ctx context.Context, grBookID int64) (*AuthorResource, error)
}

// mapper attempts to determine GR->HC mappings for author and work IDs using
// the existing api.bookinfo.pro corpus.
//
// HC currently only contains GR book IDs, so normally we can't map a GR author
// to an HC author until we've seen one book from that author.
//
// Instead, when we get a cache miss for an author or work we can load its GR
// metadata and then query HC for the book IDs associated with it.
type hcmapper struct {
	upstream *http.Client
}

func NewHCMapper(client *http.Client) (bridge, error) {
	return &hcmapper{
		upstream: client,
	}, nil
}

// MapAuthor maps a GR author ID to a foreign author ID.
func (m *hcmapper) MapAuthor(ctx context.Context, hc *HCGetter, grAuthorID int64) (string, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", fmt.Sprintf("https://api.bookinfo.pro/author/%d", grAuthorID), nil)
	if err != nil {
		return "", err
	}

	resp, err := m.upstream.Do(req)
	if err != nil {
		return "", err
	}

	d := json.NewDecoder(resp.Body)
	var author AuthorResource
	err = d.Decode(&author)
	if err != nil {
		return "", err
	}

	for _, w := range author.Works {
		workBytes, _, _, err := hc.GetBook(ctx, w.BestBookID)
		if err != nil {
			Log(ctx).Warn("problem getting book", "err", err, "grBookID", w.BestBookID)
			continue
		}
		var work workResource
		err = json.Unmarshal(workBytes, &work)
		if err != nil {
			Log(ctx).Warn("problem unmarshaling book", "err", err, "grBookID", w.BestBookID)
		}

		if work.KCA != "" {
			return work.KCA, nil
		}
	}

	return "", fmt.Errorf("no HC editions found for author %d", grAuthorID)
}

// MapWork maps a GR Work ID to a HC Book ID.
func (m *hcmapper) MapWork(ctx context.Context, grWorkID int64) (*workResource, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", fmt.Sprintf("https://api.bookinfo.pro/work/%d", grWorkID), nil)
	if err != nil {
		return nil, err
	}

	resp, err := m.upstream.Do(req)
	if err != nil {
		return nil, err
	}

	d := json.NewDecoder(resp.Body)
	var work workResource
	err = d.Decode(&work)

	return &work, err
}

func (m *hcmapper) MapEdition(ctx context.Context, grBookID int64) (*AuthorResource, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", fmt.Sprintf("https://api.bookinfo.pro/book/%d", grBookID), nil)
	if err != nil {
		return nil, err
	}

	resp, err := m.upstream.Do(req)
	if err != nil {
		return nil, err
	}

	d := json.NewDecoder(resp.Body)
	var author AuthorResource
	err = d.Decode(&author)

	return &author, err
}
