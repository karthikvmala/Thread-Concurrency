// Physical memory allocator, for user processes,
// kernel stacks, page-table pages,
// and pipe buffers. Allocates whole 4096-byte pages.

#include "types.h"
#include "param.h"
#include "memlayout.h"
#include "spinlock.h"
#include "riscv.h"
#include "defs.h"

void freerange(void *pa_start, void *pa_end);

extern char end[]; // first address after kernel.
                   // defined by kernel.ld.

struct run
{
  struct run *next;
};

struct
{
  struct spinlock lock;
  struct run *freelist;
  struct spinlock cnt_lock;
  int count[(PGROUNDUP(PHYSTOP) - KERNBASE) / PGSIZE];
} kmem;

void kinit()
{
  initlock(&kmem.lock, "kmem");
  initlock(&kmem.cnt_lock, "refcnt");
  acquire(&kmem.cnt_lock);
  for (int i = 0; i < (PGROUNDUP(PHYSTOP) - KERNBASE) / PGSIZE; i++)
  {
    kmem.count[i] = 1;
  }
  release(&kmem.cnt_lock);
  // for (int i = 0; i < (PGROUNDUP(PHYSTOP) - KERNBASE) / PGSIZE; i++)
  // {
  //   printf("%d ", refcnt.count[i]);
  // }
  freerange(end, (void *)PHYSTOP);
}
// int get_unique_index(uint64 physical_address) {
//     // A simple hash function that combines the higher and lower parts of the address
//     // This approach uses a combination of bit shifts and XOR to create more variability
//     uint32 high_part = (uint32)(physical_address >> 32); // Extract high 32 bits
//     uint32 low_part = (uint32)(physical_address & 0xFFFFFFFF); // Extract low 32 bits

//     // Combine high and low parts with XOR and bit shifts
//     uint32 hash = (high_part ^ low_part) ^ ((high_part ^ low_part) >> 12);

//     // Ensure the index is in the range [0, ARRAY_SIZE - 1]
//     return hash % 32768;
// }
void krefincr(void *pa)
{
  acquire(&kmem.cnt_lock);
  // printf("PAge table idx in fre inc %d\n", PA2IDX(pa));
  kmem.count[PA2IDX(pa)]++;
  release(&kmem.cnt_lock);
}

void krefdecr(void *pa)
{
  // printf("krefdecr called()\n");//
  acquire(&kmem.cnt_lock);
  // printf("PAge table idx in ref dec %d\n", PA2IDX(pa));
  kmem.count[PA2IDX(pa)]--;
  release(&kmem.cnt_lock);
}

int krefget(void *pa)
{
  int cnt;
  acquire(&kmem.cnt_lock);
  // printf("This is the page %lld %d\n",  get_unique_index(*(uint64 *)pa));
  cnt = kmem.count[PA2IDX(pa)];
  // printf("%d\n", cnt);
  release(&kmem.cnt_lock);
  return cnt;
}

void freerange(void *pa_start, void *pa_end)
{
  char *p;
  p = (char *)PGROUNDUP((uint64)pa_start);
  for (; p + PGSIZE <= (char *)pa_end; p += PGSIZE)
    kfree(p);
}

// Free the page of physical memory pointed at by pa,
// which normally should have been returned by a
// call to kalloc().  (The exception is when
// initializing the allocator; see kinit above.)
void kfree(void *pa)
{
  struct run *r;

  if (((uint64)pa % PGSIZE) != 0 || (char *)pa < end || (uint64)pa >= PHYSTOP)
  {
    panic("kfree");
  }

  if (krefget(pa) <= 0)
  {
    // printf("%d\n", krefget(pa));
    panic("Decrementing freed page");
  }

  krefdecr((void *)pa);
  if (krefget(pa) > 0)
  {
    return;
  }
  // Fill with junk to catch dangling refs.
  memset(pa, 1, PGSIZE);

  r = (struct run *)pa;

  acquire(&kmem.lock);
  r->next = kmem.freelist;
  kmem.freelist = r;
  release(&kmem.lock);
}

// Allocate one 4096-byte page of physical memory.
// Returns a pointer that the kernel can use.
// Returns 0 if the memory cannot be allocated.
void *
kalloc(void)
{
  struct run *r;

  acquire(&kmem.lock);
  r = kmem.freelist;
  if (r)
    kmem.freelist = r->next;
  release(&kmem.lock);

  if (r)
  {
    memset((char *)r, 5, PGSIZE); // fill with junk

    acquire(&kmem.cnt_lock);
    kmem.count[PA2IDX(r)] = 1;
    release(&kmem.cnt_lock);
  }
  return (void *)r;
}
