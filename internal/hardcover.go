package internal

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"iter"
	"strings"

	"github.com/Khan/genqlient/graphql"
	"github.com/blampe/rreading-glasses/hardcover"
)

// HCGetter implements a Getter using the Hardcover API as its source. It
// attempts to minimize upstread HEAD requests (to resolve book/work IDs) by
// relying on HC's raw external data.
type HCGetter struct {
	cache  *LayeredCache
	gql    graphql.Client
	mapper bridge
}

var _ getter = (*HCGetter)(nil)

// NewHardcoverGetter returns a new Getter backed by Hardcover.
func NewHardcoverGetter(cache *LayeredCache, gql graphql.Client, mapper bridge) (*HCGetter, error) {
	return &HCGetter{cache: cache, gql: gql, mapper: mapper}, nil
}

// GetWork returns the canonical edition for a book. Hardcover's GR mappings
// are entirely edition-based, with one edition representing the canonical
// book/work.
//
// A GR Work ID should therefore be mapped to a HC Book ID. However the HC API
// only allows us to query GR Book ID -> HC Edition ID. Therefore we query the
// shared GR instance to resolve GR Work ID -> Best GR Book ID.
//
// Once we have the GR Book ID we can return the same result as GetBook.
func (g *HCGetter) GetWork(ctx context.Context, grWorkID int64) (_ []byte, grAuthorID int64, err error) {
	workBytes, ttl, ok := g.cache.GetWithTTL(ctx, WorkKey(grWorkID))
	if ok && ttl > 0 {
		return workBytes, 0, nil
	}

	// If we previously fetched this work we should have stored its HC ID on
	// the KCA field. Use that for a direct lookup, if it's set.
	var hcWorkRsc *workResource
	if ok {
		err := json.Unmarshal(workBytes, hcWorkRsc)
		if err != nil {
			return nil, 0, err
		}
		if hcWorkRsc.BestBookID != 0 {
			grAuthorID := hcWorkRsc.Authors[0].ForeignID
			grBookID := hcWorkRsc.BestBookID
			hcWorkRsc, err := g.getEdition(ctx, grAuthorID, grWorkID, grBookID, hcWorkRsc.BestBookID)
			if err != nil {
				return nil, 0, fmt.Errorf("direct book lookup: %w", err)
			}
			hcWorkBytes, err := json.Marshal(hcWorkRsc)
			return hcWorkBytes, hcWorkRsc.Authors[0].ForeignID, err
		}
	}

	// If we don't have a HC Book ID yet, query api.bookinfo.pro for the GR
	// metadata and try to fest the work's "best" GR Book ID.
	grWorkRsc, err := g.mapper.MapWork(ctx, grWorkID)
	if err != nil {
		return nil, 0, fmt.Errorf("getting gr work: %w", err)
	}
	grAuthorID = grWorkRsc.Authors[0].ForeignID

	// Now that we have the GR metadata just return the edition corresponding
	// to GR's "best" book.
	editionBytes, _, _, err := g.GetBook(ctx, grWorkRsc.BestBookID)
	return editionBytes, grAuthorID, err
}

/*
	// We weren't able to do a direct lookup in HC based on the work's
	// BestBookID, so try to match on ISBN and finally
	// title/author.
	var bestBook *bookResource
	for _, b := range grWorkRsc.Books {
		if b.ForeignID == grWorkRsc.BestBookID {
			bestBook = &b
			break
		}
	}
	if bestBook == nil {
		return nil, 0, errors.Join(errNotFound, errors.New("best book was missing"))
	}

	edition, err := hardcover.GetEditionByISBN13(ctx, g.gql, bestBook.Isbn13)
	if err == nil && len(edition.Editions) > 0 {
		hcEditionRsc, err := g.getEdition(ctx, edition.Editions[0].Book_id)
		if err != nil {
			return nil, 0, fmt.Errorf("getting edition by isbn13: %w", err)
		}
		hcEditionBytes, err := json.Marshal(hcEditionRsc)
		return hcEditionBytes, grAuthorID, err
	}

	authorName := grWorkRsc.Authors[0].Name
	resp, err := hardcover.SearchBook(ctx, g.gql, authorName+" "+grWorkRsc.FullTitle)
	if err != nil {
		return nil, 0, fmt.Errorf("searching: %w", err)
	}

	if len(resp.Search.Ids) > 0 {
		hcWorkRsc, err := g.getBook(ctx, resp.Search.Ids[0])
		if err != nil {
			return nil, 0, fmt.Errorf("search lookup: %w", err)
		}
		hcWorkBytes, err := json.Marshal(hcWorkRsc)
		return hcWorkBytes, hcWorkRsc.Authors[0].ForeignID, err

	}

	resp.Search.Ids

	// If a work wasn't cached, attempt to look up a book in HC with the same ISBN.

	// TODO: Loading the best book ID on a cache refresh will lose any other
	// editions previously attached to this work. Instead we should re-assemble
	// the book array by re-fetching the latest books from the cache.
	if ok {
		var work workResource
		_ = json.Unmarshal(workBytes, &work)

		bookID := work.BestBookID
		if bookID != 0 {
			out, _, authorID, err := g.GetBook(ctx, bookID)
			return out, authorID, err
		}
	}

	Log(ctx).Debug("getting work", "grWorkID", grWorkID)

	bookID, err := g.mapper.MapWork(ctx, g, grWorkID)
	if err != nil {
		return nil, 0, err
	}

	workBytes, _, authorID, err := g.GetBook(ctx, bookID)
	return workBytes, authorID, err
}
*/

// GetBook looks up a GR book (edition) in Hardcover's mappings.
func (g *HCGetter) GetBook(ctx context.Context, grBookID int64) (_ []byte, grWorkID int64, grAuthorID int64, err error) {
	workBytes, ttl, ok := g.cache.GetWithTTL(ctx, BookKey(grBookID))
	if ok && ttl > 0 {
		return workBytes, 0, 0, nil
	}

	// If we previously fetched this edition we should have stored its HC ID on
	// the KCA field. Use that for a direct lookup, if it's set.
	var hcEditionRsc *workResource
	if ok {
		err := json.Unmarshal(workBytes, hcEditionRsc)
		if err != nil {
			return nil, 0, 0, err
		}
		if hcEditionID, _ := pathToID(hcEditionRsc.KCA); hcEditionID != 0 {
			grAuthorID := hcEditionRsc.Authors[0].ForeignID
			grWorkID := hcEditionRsc.ForeignID
			hcEditionRsc, err := g.getEdition(ctx, grAuthorID, grWorkID, grBookID, hcEditionID)
			if err != nil {
				return nil, 0, 0, fmt.Errorf("direct edition lookup: %w", err)
			}
			hcEditionBytes, err := json.Marshal(hcEditionRsc)
			return hcEditionBytes, grWorkID, grAuthorID, err

		}
	}

	// We weren't able to do a direct lookup in HC based on the GR book ID, so
	// try to match on ISBN, GR ID (which is not always present in HC) and
	// finally title/author.

	grAuthorRsc, err := g.mapper.MapEdition(ctx, grBookID)
	if err != nil {
		return nil, 0, 0, fmt.Errorf("getting gr edition: %w", err)
	}
	grAuthorID = grAuthorRsc.ForeignID
	grWorkID = grAuthorRsc.Works[0].ForeignID
	grBook := grAuthorRsc.Works[0].Books[0]

	// Try looking up by ISBN.
	if grBook.Isbn13 != "" {
		edition, err := hardcover.GetEditionByISBN13(ctx, g.gql, grBook.Isbn13)
		if err == nil && len(edition.Editions) > 0 {
			hcEditionRsc, err := g.getEdition(ctx, grAuthorID, grWorkID, grBookID, edition.Editions[0].Id)
			if err != nil {
				return nil, 0, 0, fmt.Errorf("getting edition by isbn13: %w", err)
			}
			hcEditionBytes, err := json.Marshal(hcEditionRsc)
			return hcEditionBytes, grWorkID, grAuthorID, err
		}
	}

	// Try looking up by external_id. This is sometimes but not always
	// available in HC book_mappings.
	edition, err := hardcover.GetEditionByGR(ctx, g.gql, fmt.Sprint(grBookID))
	if len(edition.Book_mappings) > 0 {
		hcEditionID := edition.Book_mappings[0].Edition.Id
		hcEditionRsc, err := g.getEdition(ctx, grAuthorID, grWorkID, grBookID, hcEditionID)
		if err != nil {
			return nil, 0, 0, fmt.Errorf("getting edition by isbn13: %w", err)
		}
		hcEditionBytes, err := json.Marshal(hcEditionRsc)
		return hcEditionBytes, grWorkID, grAuthorID, err
	}

	return nil, grWorkID, grAuthorID, errors.Join(errNotFound, errors.New("unable to locate a matching edition"))

	// TODO: Searching by book is awkward - we'll need to fetch the book and
	// eventually find the editionID matching our title/language/etc..
	/*
		authorName := grWorkRsc.Authors[0].Name
		resp, err := hardcover.SearchBook(ctx, g.gql, authorName+" "+grWorkRsc.FullTitle)
		if err != nil {
			return nil, grWorkID, grAuthorID, fmt.Errorf("searching: %w", err)
		}

		if len(resp.Search.Results) > 0 {
			var best struct {
				Results struct {
					Hits []struct {
						Document struct{} `json:"document"`
					} `json:"hits"`
				} `json:"results"`
			}
			err := json.Unmarshal(resp.Search.Results[0], &best)
			hcWorkRsc, err := g.getBook(ctx, resp.Search.Ids[0])
			if err != nil {
				return nil, grWorkID, grAuthorID, fmt.Errorf("search lookup: %w", err)
			}
			hcWorkBytes, err := json.Marshal(hcWorkRsc)
			return hcWorkBytes, hcWorkRsc.Authors[0].ForeignID, err

		}
	*/

	/*
		resp, err := hardcover.GetBook(ctx, g.gql, fmt.Sprint(grBookID))
		if err != nil {
			return nil, 0, 0, fmt.Errorf("getting book: %w", err)
		}

		if len(resp.Book_mappings) == 0 {
			return nil, 0, 0, errNotFound
		}
		bm := resp.Book_mappings[0]
	*/
}

func (g *HCGetter) getEdition(ctx context.Context, grAuthorID, grWorkID, grBookID, hcEditionID int64) (*workResource, error) {
	resp, err := hardcover.GetEditionByHC(ctx, g.gql, hcEditionID)

	edition := resp.Editions_by_pk
	book := edition.Book

	tags := []struct {
		Tag string `json:"tag"`
	}{}
	genres := []string{}

	err = json.Unmarshal(book.Cached_tags, &tags)
	if err != nil {
		return nil, err
	}
	for _, t := range tags {
		genres = append(genres, t.Tag)
	}
	if len(genres) == 0 {
		genres = []string{"none"}
	}

	series := []seriesResource{}
	for _, s := range book.Book_series {
		series = append(series, seriesResource{
			Title:       s.Series.Name,
			ForeignID:   s.Series.Id,
			Description: s.Series.Description,

			LinkItems: []seriesWorkLinkResource{{
				PositionInSeries: fmt.Sprint(s.Position),
				SeriesPosition:   int(s.Position), // TODO: What's the difference b/t placement?
				ForeignWorkID:    -1,              // TODO: Needs to be GR Work ID.
				Primary:          false,           // TODO: What is this?
			}},
		})
	}

	bookDescription := strings.TrimSpace(edition.Description)
	if bookDescription == "" {
		bookDescription = book.Description
	}
	if bookDescription == "" {
		bookDescription = "N/A" // Must be set.
	}

	editionTitle := edition.Title
	editionFullTitle := editionTitle
	editionSubtitle := edition.Subtitle

	if editionSubtitle != "" {
		editionTitle = strings.ReplaceAll(editionTitle, ": "+editionSubtitle, "")
		editionFullTitle = editionTitle + ": " + editionSubtitle
	}

	bookRsc := bookResource{
		ForeignID:          grBookID,
		KCA:                fmt.Sprint(edition.Id),
		Asin:               edition.Asin,
		Description:        bookDescription,
		Isbn13:             edition.Isbn_13,
		Title:              editionTitle,
		FullTitle:          editionFullTitle,
		ShortTitle:         editionTitle,
		Language:           edition.Language.Language,
		Format:             edition.Edition_format,
		EditionInformation: "",                     // TODO: Is this used anywhere?
		Publisher:          edition.Publisher.Name, // TODO: Ignore books without publishers?
		ImageURL:           strings.ReplaceAll(string(book.Cached_image), `"`, ``),
		IsEbook:            true, // TODO: Flush this out.
		NumPages:           edition.Pages,
		RatingCount:        book.Ratings_count,
		RatingSum:          int64(float64(book.Ratings_count) * book.Rating),
		AverageRating:      book.Rating,
		URL:                fmt.Sprintf("https://hardcover.app/books/%s/editions/%d", book.Slug, edition.Id),
		ReleaseDate:        edition.Release_date,

		// TODO: Grab release date from book if absent

		// TODO: Omitting release date is a way to essentially force R to hide
		// the book from the frontend while allowing the user to still add it
		// via search. Better UX depending on what you're after.
	}

	authorDescription := "N/A" // Must be set.
	author := book.Contributions[0].Author
	if author.Bio != "" {
		authorDescription = author.Bio
	}

	/*
		workID := int64(0)
		grAuthorID := int64(0)
		for _, bmbm := range book.Book_mappings {
			var dto struct {
				RawData struct {
					Work struct {
						ID int64 `json:"id"`
					} `json:"work"`
					Authors struct {
						Author struct {
							ID string `json:"id"`
						} `json:"author"`
					} `json:"authors"`
				} `json:"raw_data"`
			}
			err := json.Unmarshal(bmbm.Dto_external, &dto)
			if err != nil {
				continue
			}
			if dto.RawData.Work.ID != 0 {
				workID = dto.RawData.Work.ID
			}
			if dto.RawData.Authors.Author.ID != "" {
				grAuthorID, _ = pathToID(dto.RawData.Authors.Author.ID)
			}
			if workID != 0 && grAuthorID != 0 {
				break
			}
		}
		if workID == 0 {
			Log(ctx).Warn("upstream doesn't have a work ID", "grBookID", grBookID)
			return nil, errNotFound
		}
		if grAuthorID == 0 {
			Log(ctx).Warn("upstream doesn't have an author ID", "grBookID", grBookID)
			return nil, errNotFound
		}
	*/

	authorRsc := AuthorResource{
		KCA:         fmt.Sprint(author.Id),
		Name:        author.Name,
		ForeignID:   grAuthorID,
		URL:         "https://hardcover.app/authors/" + author.Slug,
		ImageURL:    strings.ReplaceAll(string(author.Cached_image), `"`, ``),
		Description: authorDescription,
		Series:      series, // TODO:: Doesn't fully work yet #17.
	}

	workTitle := book.Title
	workFullTitle := workTitle
	workSubtitle := book.Subtitle

	if workSubtitle != "" {
		workTitle = strings.ReplaceAll(workTitle, ": "+workSubtitle, "")
		workFullTitle = workTitle + ": " + workSubtitle
	}

	workRsc := workResource{
		KCA:          fmt.Sprint(rune(book.Id)),
		Title:        workTitle,
		FullTitle:    workFullTitle,
		ShortTitle:   workTitle,
		BestBookID:   book.Default_cover_edition_id,
		ForeignID:    grWorkID,
		URL:          "https://hardcover.app/books/" + book.Slug,
		ReleaseDate:  book.Release_date,
		Series:       series,
		Genres:       genres,
		RelatedWorks: []int{},
	}

	bookRsc.Contributors = []contributorResource{{ForeignID: grAuthorID, Role: "Author"}}
	authorRsc.Works = []workResource{workRsc}
	workRsc.Authors = []AuthorResource{authorRsc}
	workRsc.Books = []bookResource{bookRsc} // TODO: Add best book here as well?

	return &workRsc, nil
}

// GetAuthorBooks returns all GR book (edition) IDs.
func (g *HCGetter) GetAuthorBooks(ctx context.Context, authorID int64) iter.Seq[int64] {
	noop := func(yield func(int64) bool) {}
	authorBytes, ok := g.cache.Get(ctx, AuthorKey(authorID))
	if !ok {
		Log(ctx).Debug("skipping uncached author", "authorID", authorID)
		return noop
	}

	var author AuthorResource
	err := json.Unmarshal(authorBytes, &author)
	if err != nil {
		Log(ctx).Warn("problem unmarshaling author", "authorID", authorID)
		return noop
	}

	hcAuthorID, _ := pathToID(author.KCA)

	return func(yield func(int64) bool) {
		limit, offset := int64(20), int64(0)
		for {
			gae, err := hardcover.GetAuthorEditions(ctx, g.gql, hcAuthorID, limit, offset)
			if err != nil {
				Log(ctx).Warn("problem getting author editions", "err", err, "authorID", authorID)
				return
			}

			hcAuthor := gae.Authors_by_pk
			for _, c := range hcAuthor.Contributions {
				if len(c.Book.Book_mappings) == 0 {
					Log(ctx).Debug("no mappings found")
					continue
				}

				grAuthorID, _ := pathToID(string(hcAuthor.Identifiers))
				if grAuthorID != authorID {
					Log(ctx).Debug("skipping unrelated author", "want", authorID, "got", grAuthorID)
					continue
				}

				externalID := c.Book.Book_mappings[0].External_id
				grBookID, err := pathToID(externalID)
				if err != nil {
					Log(ctx).Warn("unexpected ID error", "err", err, "externalID", externalID)
					continue
				}

				if !yield(grBookID) {
					return
				}
			}

			// This currently returns a ton of stuff including translated works. So we
			// stop prematurely instead of loading all of it for now.
			// offset += limit
			break
		}
	}
}

// GetAuthor looks up a GR author on Hardcover. The HC API doesn't track GR
// author IDs, so we only become aware of the HC ID once one of the author's
// books is queried in GetBook.
func (g *HCGetter) GetAuthor(ctx context.Context, grAuthorID int64) ([]byte, error) {
	var hcAuthorID string

	authorBytes, ok := g.cache.Get(ctx, AuthorKey(grAuthorID))

	if ok {
		var author AuthorResource
		_ = json.Unmarshal(authorBytes, &author)
		hcAuthorID = author.KCA
		if hcAuthorID != "" {
			Log(ctx).Debug("found cached author", "hcAuthorID", hcAuthorID, "authorID", grAuthorID)
		}
	}

	if hcAuthorID == "" {
		var err error
		hcAuthorID, err = g.mapper.MapAuthor(ctx, g, grAuthorID)
		if err != nil {
			Log(ctx).Warn("problem seeding author", "err", err, "grAuthorID", grAuthorID)
			return nil, err
		}
	}

	if hcAuthorID == "" {
		Log(ctx).Warn("unable to resolve author", "grAuthorID", grAuthorID, "hit", ok)
		return nil, fmt.Errorf("unable to resolve author %d", grAuthorID)

	}

	aid, _ := pathToID(hcAuthorID)
	gar, err := hardcover.GetAuthor(ctx, g.gql, aid)
	if err != nil {
		return nil, fmt.Errorf("getting author: %w", err)
	}

	authorRsc := AuthorResource{
		KCA:         fmt.Sprint(gar.Authors_by_pk.Id),
		Name:        gar.Authors_by_pk.Name,
		ForeignID:   grAuthorID,
		URL:         "https://hardcover.app/authors/" + gar.Authors_by_pk.Slug,
		ImageURL:    strings.ReplaceAll(string(gar.Authors_by_pk.Cached_image), `"`, ``),
		Description: gar.Authors_by_pk.Bio,
		Works:       []workResource{},
	}

	grBookIDs, err := hardcover.GetAuthorEditions(ctx, g.gql, grAuthorID, 20, 0)
	if err != nil {
		return nil, err
	}
	for _, cont := range grBookIDs.Authors_by_pk.Contributions {
		if len(cont.Book.Book_mappings) == 0 {
			Log(ctx).Warn("no book mappings found", "grAuthorID", grAuthorID)
			continue
		}

		grBookID, _ := pathToID(cont.Book.Book_mappings[0].External_id)
		workBytes, _, _, err := g.GetBook(ctx, grBookID)
		if err != nil {
			Log(ctx).Warn("getting edition", "err", err, "grBooKID", grBookID)
			continue
		}
		var workRsc workResource
		err = json.Unmarshal(workBytes, &workRsc)
		if err != nil {
			authorRsc.Works = []workResource{workRsc}
			break
		}
	}

	return json.Marshal(authorRsc)
}
